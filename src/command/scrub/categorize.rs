//! Pure key categorization for the scrub walk.
//!
//! [`categorize`] maps a raw object-store key onto the union of both stores'
//! layouts (the blob and metadata stores can share one physical root, so a
//! walk of either may see the other's keys). It performs no I/O; validation
//! of an object's content and references happens in `validate`.
//!
//! A key that matches no known shape is [`KeyCategory::Unknown`] and gets
//! quarantined under the lost-and-found prefix, so a shape a newer angos
//! version writes is recoverable rather than destroyed.

use std::str::FromStr;

use angos_tx_engine::{
    INTENT_BODIES_PREFIX, INTENT_LOG_PREFIX, LOCK_OBJECTS_PREFIX, PROBE_KEY_PREFIX,
};

use crate::{
    jobs::{JobState, Queue, store::JOBS_ROOT},
    oci::{Algorithm, Digest},
    registry::{
        metadata_store::decode_blob_index_shard_namespace,
        path_builder::{blobs_root_dir, repository_dir},
    },
};

use super::action::LOST_AND_FOUND_PREFIX;

/// Everything a key in either store can be.
#[derive(Debug, PartialEq, Eq)]
pub enum KeyCategory {
    /// `v2/blobs/{alg}/{prefix}/{hash}/data` (blob store).
    BlobData { digest: Digest },
    /// `v2/blobs/{alg}/{prefix}/{hash}/refs/{encoded-ns}.json` (metadata store).
    BlobIndexShard { digest: Digest, namespace: String },
    /// A link file under `v2/repositories/{ns}/...` (metadata store). The
    /// namespace is raw: its validity is a validation concern.
    Link { namespace: String, link: ParsedLink },
    /// An upload-session artifact under `v2/repositories/{ns}/_uploads/{uuid}/`
    /// (blob store).
    UploadArtifact {
        namespace: String,
        uuid: String,
        artifact: UploadArtifact,
    },
    /// A pending or dead-lettered job envelope (metadata store).
    JobRecord { queue: Queue, state: JobState },
    /// A `lock_key` dedup index entry (metadata store).
    JobIndex { queue: Queue },
    /// Transaction-engine state (`.tx-log/`, `.tx-bodies/`, `.tx-locks/`).
    /// The engine's recovery loop and janitors own these; scrub must never
    /// touch them.
    EngineInternal,
    /// Already quarantined; never re-processed.
    LostAndFound,
    /// A leaked startup CAS-probe object at the store root.
    Probe,
    /// Matches no known angos layout.
    Unknown,
}

/// A link-shaped key, parsed by path grammar only (body parsing is a
/// validation concern).
#[derive(Debug, PartialEq, Eq)]
pub enum ParsedLink {
    /// `_manifests/tags/{name}/current/link`. The name is raw: an invalid tag
    /// directory is a categorized defect (deleted by validation), not an
    /// unknown key.
    Tag { name: String },
    /// `_manifests/revisions/{alg}/{hash}/link`.
    Revision(Digest),
    /// `_blobs/{alg}/{hash}/link`.
    Blob(Digest),
    /// `_layers/{alg}/{hash}/link`.
    Layer(Digest),
    /// `_config/{alg}/{hash}/link`.
    Config(Digest),
    /// `_manifests/referrers/{subject}/{referrer}/link`.
    Referrer { subject: Digest, referrer: Digest },
    /// `_manifests/index/{index}/{child}/link`.
    ManifestIndex { index: Digest, child: Digest },
}

/// The per-file artifacts of one upload session.
#[derive(Debug, PartialEq, Eq)]
pub enum UploadArtifact {
    /// `data`: the assembled upload bytes.
    Data,
    /// `startedat`: RFC3339 last-activity marker.
    StartedAt,
    /// `hashstates/{offset}`: a resumable-hash checkpoint.
    HashState,
    /// `staged/{offset}`: an S3 multipart sub-part remainder.
    Staged,
}

/// Namespace markers: the reserved first path segment after the namespace in
/// a repository key. Valid namespace components never start with `_`, so the
/// first marker segment unambiguously ends the namespace.
const NAMESPACE_MARKERS: [&str; 5] = ["_uploads", "_manifests", "_blobs", "_layers", "_config"];

/// Categorize a raw store key against the union of both stores' layouts.
pub fn categorize(key: &str) -> KeyCategory {
    // A degenerate segment can never come from an angos writer and could
    // escape a directory boundary if echoed into a path; refuse the shape.
    if key.is_empty()
        || key
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return KeyCategory::Unknown;
    }

    if strip_prefix_dir(key, INTENT_LOG_PREFIX).is_some()
        || strip_prefix_dir(key, INTENT_BODIES_PREFIX).is_some()
        || strip_prefix_dir(key, LOCK_OBJECTS_PREFIX).is_some()
    {
        return KeyCategory::EngineInternal;
    }
    if strip_prefix_dir(key, LOST_AND_FOUND_PREFIX).is_some() {
        return KeyCategory::LostAndFound;
    }
    if !key.contains('/') && key.starts_with(PROBE_KEY_PREFIX) {
        return KeyCategory::Probe;
    }
    if let Some(rest) = strip_prefix_dir(key, JOBS_ROOT) {
        return categorize_job(rest);
    }
    if let Some(rest) = strip_prefix_dir(key, blobs_root_dir()) {
        return categorize_blob(rest);
    }
    if let Some(rest) = strip_prefix_dir(key, repository_dir()) {
        return categorize_repository(rest);
    }

    KeyCategory::Unknown
}

/// The remainder of `key` below the directory `prefix`. `None` when `key` is
/// not under it; a bare string prefix never matches (`v2/blobsx` is not under
/// `v2/blobs`).
fn strip_prefix_dir<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    rest.strip_prefix('/')
}

/// `{alg}/{prefix}/{hash}/data` or `{alg}/{prefix}/{hash}/refs/{ns}.json`.
fn categorize_blob(rest: &str) -> KeyCategory {
    let segments: Vec<&str> = rest.split('/').collect();
    let [algorithm, prefix, hash, tail @ ..] = segments.as_slice() else {
        return KeyCategory::Unknown;
    };
    let Some(digest) = parse_digest(algorithm, hash) else {
        return KeyCategory::Unknown;
    };
    if hash.as_bytes().get(..2) != Some(prefix.as_bytes()) {
        return KeyCategory::Unknown;
    }

    match *tail {
        ["data"] => KeyCategory::BlobData { digest },
        ["refs", shard] => match shard.strip_suffix(".json") {
            Some(encoded) => KeyCategory::BlobIndexShard {
                digest,
                namespace: decode_blob_index_shard_namespace(encoded),
            },
            None => KeyCategory::Unknown,
        },
        _ => KeyCategory::Unknown,
    }
}

/// `pending/{queue}/{stem}.json`, `failed/{queue}/{stem}.json`, or
/// `index/{queue}/{encoded}.json`.
fn categorize_job(rest: &str) -> KeyCategory {
    let segments: Vec<&str> = rest.split('/').collect();
    let [partition, queue, file] = segments.as_slice() else {
        return KeyCategory::Unknown;
    };
    // An unknown queue name may belong to a newer angos; quarantine, never
    // delete.
    let Ok(queue) = Queue::from_str(queue) else {
        return KeyCategory::Unknown;
    };
    if file.strip_suffix(".json").is_none() {
        return KeyCategory::Unknown;
    }

    match *partition {
        "pending" => KeyCategory::JobRecord {
            queue,
            state: JobState::Pending,
        },
        "failed" => KeyCategory::JobRecord {
            queue,
            state: JobState::Failed,
        },
        "index" => KeyCategory::JobIndex { queue },
        _ => KeyCategory::Unknown,
    }
}

/// `{ns...}/{marker}/{...}` where `{ns...}` is one or more namespace segments
/// and `{marker}` is the first reserved `_`-segment.
fn categorize_repository(rest: &str) -> KeyCategory {
    let segments: Vec<&str> = rest.split('/').collect();
    let Some(marker_at) = segments
        .iter()
        .position(|segment| NAMESPACE_MARKERS.contains(segment))
    else {
        return KeyCategory::Unknown;
    };
    if marker_at == 0 {
        // No namespace before the marker; not addressable by any angos API.
        return KeyCategory::Unknown;
    }
    let namespace = segments[..marker_at].join("/");
    let tail = &segments[marker_at + 1..];

    match segments[marker_at] {
        "_uploads" => categorize_upload(namespace, tail),
        "_blobs" => single_digest_link(namespace, tail, ParsedLink::Blob),
        "_layers" => single_digest_link(namespace, tail, ParsedLink::Layer),
        "_config" => single_digest_link(namespace, tail, ParsedLink::Config),
        "_manifests" => categorize_manifest(namespace, tail),
        _ => KeyCategory::Unknown,
    }
}

/// `{uuid}/data`, `{uuid}/startedat`, `{uuid}/hashstates/{offset}`, or
/// `{uuid}/staged/{offset}`.
fn categorize_upload(namespace: String, tail: &[&str]) -> KeyCategory {
    let (uuid, artifact) = match tail {
        [uuid, "data"] => (uuid, UploadArtifact::Data),
        [uuid, "startedat"] => (uuid, UploadArtifact::StartedAt),
        [uuid, "hashstates", offset] if offset.parse::<u64>().is_ok() => {
            (uuid, UploadArtifact::HashState)
        }
        [uuid, "staged", offset] if offset.parse::<u64>().is_ok() => (uuid, UploadArtifact::Staged),
        _ => return KeyCategory::Unknown,
    };
    KeyCategory::UploadArtifact {
        namespace,
        uuid: (*uuid).to_string(),
        artifact,
    }
}

/// `tags/{name}/current/link`, `revisions/{alg}/{hash}/link`,
/// `referrers/{s-alg}/{s-hash}/{r-alg}/{r-hash}/link`, or
/// `index/{i-alg}/{i-hash}/{c-alg}/{c-hash}/link`.
fn categorize_manifest(namespace: String, tail: &[&str]) -> KeyCategory {
    let link = match tail {
        ["tags", name, "current", "link"] => ParsedLink::Tag {
            name: (*name).to_string(),
        },
        ["revisions", algorithm, hash, "link"] => match parse_digest(algorithm, hash) {
            Some(digest) => ParsedLink::Revision(digest),
            None => return KeyCategory::Unknown,
        },
        [
            "referrers",
            s_algorithm,
            s_hash,
            r_algorithm,
            r_hash,
            "link",
        ] => {
            match (
                parse_digest(s_algorithm, s_hash),
                parse_digest(r_algorithm, r_hash),
            ) {
                (Some(subject), Some(referrer)) => ParsedLink::Referrer { subject, referrer },
                _ => return KeyCategory::Unknown,
            }
        }
        ["index", i_algorithm, i_hash, c_algorithm, c_hash, "link"] => {
            match (
                parse_digest(i_algorithm, i_hash),
                parse_digest(c_algorithm, c_hash),
            ) {
                (Some(index), Some(child)) => ParsedLink::ManifestIndex { index, child },
                _ => return KeyCategory::Unknown,
            }
        }
        _ => return KeyCategory::Unknown,
    };
    KeyCategory::Link { namespace, link }
}

/// `{alg}/{hash}/link` for the single-digest link kinds.
fn single_digest_link(
    namespace: String,
    tail: &[&str],
    build: fn(Digest) -> ParsedLink,
) -> KeyCategory {
    let [algorithm, hash, "link"] = tail else {
        return KeyCategory::Unknown;
    };
    match parse_digest(algorithm, hash) {
        Some(digest) => KeyCategory::Link {
            namespace,
            link: build(digest),
        },
        None => KeyCategory::Unknown,
    }
}

/// A digest from separate path segments; `None` (an unknown algorithm or a
/// malformed hash) means the key cannot belong to this angos version.
fn parse_digest(algorithm: &str, hash: &str) -> Option<Digest> {
    let algorithm = Algorithm::from_str(algorithm).ok()?;
    Digest::with_algorithm(algorithm, hash).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        jobs::store::{job_failed_path, job_lock_key_index_path, job_pending_path},
        oci::{Namespace, Tag},
        registry::{
            metadata_store::LinkKind,
            path_builder::{
                blob_index_shard_path, blob_path, link_path, upload_hash_context_path, upload_path,
                upload_start_date_path,
            },
        },
    };

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn digest_a() -> Digest {
        Digest::sha256(HASH_A).unwrap()
    }

    fn digest_b() -> Digest {
        Digest::sha256(HASH_B).unwrap()
    }

    fn namespace() -> Namespace {
        Namespace::new("org/app").unwrap()
    }

    #[test]
    fn blob_data_path_round_trips() {
        assert_eq!(
            categorize(&blob_path(&digest_a())),
            KeyCategory::BlobData { digest: digest_a() }
        );
    }

    #[test]
    fn blob_index_shard_path_round_trips_with_multi_segment_namespace() {
        assert_eq!(
            categorize(&blob_index_shard_path(&digest_a(), &namespace())),
            KeyCategory::BlobIndexShard {
                digest: digest_a(),
                namespace: "org/app".to_string(),
            }
        );
    }

    #[test]
    fn every_link_kind_round_trips() {
        let cases: Vec<(LinkKind, ParsedLink)> = vec![
            (
                LinkKind::Tag(Tag::new("v1.0").unwrap()),
                ParsedLink::Tag {
                    name: "v1.0".to_string(),
                },
            ),
            (
                LinkKind::Digest(digest_a()),
                ParsedLink::Revision(digest_a()),
            ),
            (LinkKind::Blob(digest_a()), ParsedLink::Blob(digest_a())),
            (LinkKind::Layer(digest_a()), ParsedLink::Layer(digest_a())),
            (LinkKind::Config(digest_a()), ParsedLink::Config(digest_a())),
            (
                LinkKind::Referrer(digest_a(), digest_b()),
                ParsedLink::Referrer {
                    subject: digest_a(),
                    referrer: digest_b(),
                },
            ),
            (
                LinkKind::Manifest(digest_a(), digest_b()),
                ParsedLink::ManifestIndex {
                    index: digest_a(),
                    child: digest_b(),
                },
            ),
        ];
        for (kind, expected) in cases {
            assert_eq!(
                categorize(&link_path(&kind, &namespace())),
                KeyCategory::Link {
                    namespace: "org/app".to_string(),
                    link: expected,
                },
                "link kind {kind:?} must round-trip"
            );
        }
    }

    #[test]
    fn upload_artifacts_round_trip() {
        let ns = namespace();
        let cases = [
            (upload_path(&ns, "uuid-1"), UploadArtifact::Data),
            (
                upload_start_date_path(&ns, "uuid-1"),
                UploadArtifact::StartedAt,
            ),
            (
                upload_hash_context_path(&ns, "uuid-1", 42),
                UploadArtifact::HashState,
            ),
            (
                "v2/repositories/org/app/_uploads/uuid-1/staged/7".to_string(),
                UploadArtifact::Staged,
            ),
        ];
        for (key, expected) in cases {
            assert_eq!(
                categorize(&key),
                KeyCategory::UploadArtifact {
                    namespace: "org/app".to_string(),
                    uuid: "uuid-1".to_string(),
                    artifact: expected,
                },
                "upload artifact {key} must round-trip"
            );
        }
    }

    #[test]
    fn job_paths_round_trip() {
        assert_eq!(
            categorize(&job_pending_path("replication", "0000-id")),
            KeyCategory::JobRecord {
                queue: Queue::Replication,
                state: JobState::Pending,
            }
        );
        assert_eq!(
            categorize(&job_failed_path("cache", "0000-id")),
            KeyCategory::JobRecord {
                queue: Queue::Cache,
                state: JobState::Failed,
            }
        );
        assert_eq!(
            categorize(&job_lock_key_index_path("cache", "a/b:c")),
            KeyCategory::JobIndex {
                queue: Queue::Cache,
            }
        );
    }

    #[test]
    fn engine_and_reserved_prefixes_are_recognized() {
        assert_eq!(
            categorize(".tx-log/0000-uuid.json"),
            KeyCategory::EngineInternal
        );
        assert_eq!(categorize(".tx-bodies/uuid/0"), KeyCategory::EngineInternal);
        assert_eq!(
            categorize(".tx-locks/aa/some-key"),
            KeyCategory::EngineInternal
        );
        assert_eq!(
            categorize("_lost_and_found/foo/bar"),
            KeyCategory::LostAndFound
        );
        assert_eq!(categorize("_angos_probe_1234"), KeyCategory::Probe);
    }

    #[test]
    fn adversarial_keys_are_unknown() {
        let unknown = [
            // Root garbage and near-miss string prefixes.
            "garbage",
            "v2/blobsx/sha256/aa/file",
            "v2/blobs",
            "v2/repositories",
            // Blob shard-prefix / hash mismatches and truncations.
            &format!("v2/blobs/sha256/bb/{HASH_A}/data"),
            "v2/blobs/sha256/aa/aaaa/data",
            &format!("v2/blobs/sha3/aa/{HASH_A}/data"),
            &format!("v2/blobs/sha256/aa/{HASH_A}/other"),
            &format!("v2/blobs/sha256/aa/{HASH_A}/refs/ns"),
            // Repository keys with no namespace, no marker, or a stray tail.
            "v2/repositories/_manifests/tags/x/current/link",
            "v2/repositories/ns/unmarked/file",
            &format!("v2/repositories/ns/_manifests/revisions/sha256/{HASH_A}/extra"),
            "v2/repositories/ns/_manifests/tags/a/b/current/link",
            "v2/repositories/ns/_uploads/uuid/hashstates/not-a-number",
            "v2/repositories/ns/_uploads/uuid/junkfile",
            // Jobs with an unknown queue or partition, or a non-JSON file.
            "_jobs/pending/futurequeue/0000-id.json",
            "_jobs/leases/replication/0000-id.json",
            "_jobs/pending/replication/0000-id",
            // Probe-like keys below the root.
            "sub/_angos_probe_1234",
            // Degenerate segments.
            "v2//blobs/x",
            "v2/repositories/../escape",
            "",
        ];
        for key in unknown {
            assert_eq!(categorize(key), KeyCategory::Unknown, "key {key:?}");
        }
    }

    #[test]
    fn invalid_tag_name_is_still_a_categorized_tag_link() {
        // A tag directory whose name fails the tag grammar is a known defect
        // handled by validation (deleted), not an unknown key.
        assert_eq!(
            categorize("v2/repositories/ns/_manifests/tags/-bad/current/link"),
            KeyCategory::Link {
                namespace: "ns".to_string(),
                link: ParsedLink::Tag {
                    name: "-bad".to_string(),
                },
            }
        );
    }
}
