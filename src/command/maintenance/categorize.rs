//! Pure key categorization for the maintenance walks (scrub, prune's sweeps).
//!
//! [`categorize`] maps a raw key onto the union of both stores' layouts, since
//! they may share one physical root, and performs no I/O. A key matching no
//! known shape is [`KeyCategory::Unknown`] and gets quarantined, so a shape a
//! newer angos version writes is recoverable rather than destroyed.

use std::str::FromStr;

use angos_oci::{Algorithm, Digest, Namespace, Tag, UploadSessionId};

use crate::command::maintenance::action::LOST_AND_FOUND_PREFIX;
use crate::{
    jobs::{JobState, Queue, store::JOBS_ROOT},
    registry::{
        keys::DigestKeys,
        keys::{BLOBS_ROOT, CAT_ROOT, GC_ROOT, LAYERS_ROOT, NS_ROOT, REF_ROOT, REPOS_ROOT},
        metadata_store::{LinkKind, parse_atime_entry, parse_tag_entry},
    },
};

/// Everything a key in either store can be.
#[derive(Debug, PartialEq, Eq)]
pub enum KeyCategory {
    /// `v2/blobs/{alg}/{prefix}/{hash}/data` (blob store).
    BlobData { digest: Digest },
    /// `v2/layers/{alg}/{prefix}/{hash}/entries` or `/checkpoints` (metadata
    /// store): a layer's filesystem listing, derived from its blob.
    LayerListing { digest: Digest },
    /// `v2/ref/{alg}/{prefix}/{hash}/{ns}!own` or `.../{ns}!r/{entry}`
    /// (metadata store); the namespace is raw, its validity being a validation
    /// concern.
    BlobRef {
        digest: Digest,
        namespace: String,
        link: LinkKind,
    },
    /// `v2/ns/{ns}!tag/{tag}!/{ord}.{kind}.{alg}.{hash}` (metadata store): one
    /// write-once tag event, both names already checked against their grammars.
    TagEntry { namespace: String, tag: String },
    /// `v2/ns/{ns}!hist/{tag}!/{ord}.{kind}.{alg}.{hash}` (metadata store):
    /// one demoted tag-history entry, write-once and never validated.
    TagHistory,
    /// `v2/ns/{ns}!atime/tag/{tag}!/{ord}.{suffix}` (metadata store): one
    /// append-only tag access entry.
    TagAtimeEntry { namespace: String, tag: String },
    /// `v2/ns/{ns}!atime/rev/{alg}/{hash}!/{ord}.{suffix}` (metadata store):
    /// one append-only revision access entry.
    RevisionAtimeEntry { namespace: String, digest: Digest },
    /// `v2/ns/{ns}!rev/{alg}/{prefix}/{hash}` (metadata store): the immutable
    /// record of a stored manifest revision.
    RevisionRecord { namespace: String, digest: Digest },
    /// `v2/ns/{ns}!sub/{alg}/{prefix}/{hash}/{r-alg}.{r-hash}` (metadata
    /// store): one referrer record under its subject.
    ReferrerRecord {
        namespace: String,
        subject: Digest,
        referrer: Digest,
    },
    /// `v2/cat/{ns}!` (metadata store): a namespace's catalog index key.
    CatalogIndex { namespace: String },
    /// An upload-session artifact under `v2/repositories/{ns}/_uploads/{uuid}/`
    /// (blob store).
    UploadArtifact {
        namespace: String,
        artifact: UploadArtifact,
    },
    /// A pending or dead-lettered job envelope (metadata store).
    JobRecord { queue: Queue, state: JobState },
    /// A `lock_key` dedup index entry (metadata store).
    JobIndex { queue: Queue },
    /// A worker's leased claim key under `_jobs/claims/` (metadata store).
    JobClaim,
    /// A collector run marker under `v2/gc/` (metadata store), the one key a
    /// writer and the collector must both observe. The walk never touches it;
    /// a crashed run's marker expires by its own TTL.
    GcMarker,
    /// Already quarantined; never re-processed.
    LostAndFound,
    /// A leaked startup CAS-probe object at the store root.
    Probe,
    /// Matches no known angos layout.
    Unknown,
}

/// The per-file artifacts of one upload session.
#[derive(Debug, PartialEq, Eq)]
pub enum UploadArtifact {
    /// `data`: the assembled upload bytes.
    Data,
    /// `session.json`: the session's single durable record.
    SessionJson,
    /// `staged/{offset}`: an S3 multipart sub-part remainder.
    Staged,
}

/// Prefix of the startup CAS-probe objects at the store root.
const PROBE_KEY_PREFIX: &str = "_angos_probe_";

/// The reserved first path segment after the namespace in a repository key.
/// Valid namespace components never start with `_`, so the first marker
/// segment unambiguously ends the namespace.
const NAMESPACE_MARKERS: [&str; 1] = ["_uploads"];

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

    if strip_prefix_dir(key, LOST_AND_FOUND_PREFIX).is_some() {
        return KeyCategory::LostAndFound;
    }
    if !key.contains('/') && key.starts_with(PROBE_KEY_PREFIX) {
        return KeyCategory::Probe;
    }
    if let Some(rest) = strip_prefix_dir(key, GC_ROOT) {
        return if rest.contains('/') {
            KeyCategory::Unknown
        } else {
            KeyCategory::GcMarker
        };
    }
    if let Some(rest) = strip_prefix_dir(key, JOBS_ROOT) {
        return categorize_job(rest);
    }
    if let Some(rest) = strip_prefix_dir(key, BLOBS_ROOT) {
        return categorize_blob(rest);
    }
    if let Some(rest) = strip_prefix_dir(key, LAYERS_ROOT) {
        return categorize_layer(rest);
    }
    if let Some(rest) = strip_prefix_dir(key, REF_ROOT) {
        return categorize_ref(rest);
    }
    if let Some(rest) = strip_prefix_dir(key, NS_ROOT) {
        return categorize_ns(rest);
    }
    if let Some(rest) = strip_prefix_dir(key, CAT_ROOT) {
        return match rest.strip_suffix('!') {
            Some(name) if Namespace::new(name).is_ok() => KeyCategory::CatalogIndex {
                namespace: name.to_string(),
            },
            _ => KeyCategory::Unknown,
        };
    }
    if let Some(rest) = strip_prefix_dir(key, REPOS_ROOT) {
        return categorize_repository(rest);
    }

    KeyCategory::Unknown
}

/// The remainder of `key` below the directory `prefix`, matching on segment
/// boundaries only (`v2/blobsx` is not under `v2/blobs`).
fn strip_prefix_dir<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    rest.strip_prefix('/')
}

/// `{alg}/{prefix}/{hash}/data`.
fn categorize_blob(rest: &str) -> KeyCategory {
    let segments: Vec<&str> = rest.split('/').collect();
    let [algorithm, prefix, hash, tail @ ..] = segments.as_slice() else {
        return KeyCategory::Unknown;
    };
    let Some(digest) = parse_sharded(algorithm, prefix, hash) else {
        return KeyCategory::Unknown;
    };

    match *tail {
        ["data"] => KeyCategory::BlobData { digest },
        _ => KeyCategory::Unknown,
    }
}

/// `{alg}/{prefix}/{hash}/entries` or `{alg}/{prefix}/{hash}/checkpoints`.
fn categorize_layer(rest: &str) -> KeyCategory {
    let segments: Vec<&str> = rest.split('/').collect();
    let [algorithm, prefix, hash, tail @ ..] = segments.as_slice() else {
        return KeyCategory::Unknown;
    };
    let Some(digest) = parse_sharded(algorithm, prefix, hash) else {
        return KeyCategory::Unknown;
    };

    match *tail {
        ["entries" | "checkpoints"] => KeyCategory::LayerListing { digest },
        _ => KeyCategory::Unknown,
    }
}

/// `{alg}/{prefix}/{hash}/{ns}!own` or `{alg}/{prefix}/{hash}/{ns}!r/{entry}`.
fn categorize_ref(rest: &str) -> KeyCategory {
    let mut segments = rest.splitn(4, '/');
    let (Some(algorithm), Some(prefix), Some(hash), Some(tail)) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return KeyCategory::Unknown;
    };
    let Some(digest) = parse_sharded(algorithm, prefix, hash) else {
        return KeyCategory::Unknown;
    };
    match digest.parse_blob_ref(tail) {
        Some((namespace, link)) => KeyCategory::BlobRef {
            digest,
            namespace,
            link,
        },
        None => KeyCategory::Unknown,
    }
}

/// `{ns}!tag/{tag}!/{ord}.{kind}.{alg}.{hash}` or `{ns}!atime/tag/{tag}`.
/// Grammars are checked here, so a shape no angos writer can produce is
/// quarantined rather than trusted.
fn categorize_ns(rest: &str) -> KeyCategory {
    let Some((namespace, marker)) = rest.split_once('!') else {
        return KeyCategory::Unknown;
    };
    if Namespace::new(namespace).is_err() {
        return KeyCategory::Unknown;
    }
    if let Some((kind @ ("tag" | "hist"), entries)) = marker.split_once('/') {
        let Some((tag, entry)) = entries.split_once("!/") else {
            return KeyCategory::Unknown;
        };
        if Tag::new(tag).is_err() || parse_tag_entry(entry).is_none() {
            return KeyCategory::Unknown;
        }
        return if kind == "tag" {
            KeyCategory::TagEntry {
                namespace: namespace.to_string(),
                tag: tag.to_string(),
            }
        } else {
            KeyCategory::TagHistory
        };
    }
    if let Some(rest) = marker.strip_prefix("atime/") {
        return categorize_atime(namespace, rest);
    }
    if let Some(rest) = marker.strip_prefix("rev/") {
        let segments: Vec<&str> = rest.split('/').collect();
        let [algorithm, prefix, hash] = segments.as_slice() else {
            return KeyCategory::Unknown;
        };
        let Some(digest) = parse_sharded(algorithm, prefix, hash) else {
            return KeyCategory::Unknown;
        };
        return KeyCategory::RevisionRecord {
            namespace: namespace.to_string(),
            digest,
        };
    }
    if let Some(rest) = marker.strip_prefix("sub/") {
        let segments: Vec<&str> = rest.split('/').collect();
        let [algorithm, prefix, hash, entry] = segments.as_slice() else {
            return KeyCategory::Unknown;
        };
        let Some(subject) = parse_sharded(algorithm, prefix, hash) else {
            return KeyCategory::Unknown;
        };
        let Some((r_algorithm, r_hash)) = entry.split_once('.') else {
            return KeyCategory::Unknown;
        };
        let Some(referrer) = parse_digest(r_algorithm, r_hash) else {
            return KeyCategory::Unknown;
        };
        return KeyCategory::ReferrerRecord {
            namespace: namespace.to_string(),
            subject,
            referrer,
        };
    }
    KeyCategory::Unknown
}

/// `tag/{tag}!/{ord}.{suffix}` or `rev/{alg}/{hash}!/{ord}.{suffix}`: one
/// append-only access entry.
fn categorize_atime(namespace: &str, rest: &str) -> KeyCategory {
    if let Some(tag_rest) = rest.strip_prefix("tag/") {
        if let Some((tag, entry)) = tag_rest.split_once("!/")
            && Tag::new(tag).is_ok()
            && parse_atime_entry(entry).is_some()
        {
            return KeyCategory::TagAtimeEntry {
                namespace: namespace.to_string(),
                tag: tag.to_string(),
            };
        }
        return KeyCategory::Unknown;
    }
    if let Some(rev_rest) = rest.strip_prefix("rev/")
        && let Some((target, entry)) = rev_rest.split_once("!/")
    {
        let mut parts = target.splitn(2, '/');
        if let (Some(algorithm), Some(hash)) = (parts.next(), parts.next())
            && let Some(digest) = parse_digest(algorithm, hash)
            && parse_atime_entry(entry).is_some()
        {
            return KeyCategory::RevisionAtimeEntry {
                namespace: namespace.to_string(),
                digest,
            };
        }
    }
    KeyCategory::Unknown
}

/// `pending/{queue}/{stem}.json`, `failed/{queue}/{stem}.json`,
/// `index/{queue}/{encoded}.json`, or `claims/{encoded}.json`.
fn categorize_job(rest: &str) -> KeyCategory {
    // Claim keys are worker leases the walk never touches; a lapsed one is
    // taken over by the next claimant.
    if let Some(file) = rest.strip_prefix("claims/")
        && !file.contains('/')
    {
        return KeyCategory::JobClaim;
    }
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
        _ => KeyCategory::Unknown,
    }
}

/// `{uuid}/data`, `{uuid}/session.json`, or `{uuid}/staged/{offset}`.
fn categorize_upload(namespace: String, tail: &[&str]) -> KeyCategory {
    let (session_id, artifact) = match tail {
        [session_id, "data"] => (session_id, UploadArtifact::Data),
        [session_id, "session.json"] => (session_id, UploadArtifact::SessionJson),
        [session_id, "staged", offset] if offset.parse::<u64>().is_ok() => {
            (session_id, UploadArtifact::Staged)
        }
        _ => return KeyCategory::Unknown,
    };
    // A directory angos never opened belongs in the unknown-key quarantine,
    // not in a session the upload passes can address.
    if UploadSessionId::from_str(session_id).is_err() {
        return KeyCategory::Unknown;
    }
    KeyCategory::UploadArtifact {
        namespace,
        artifact,
    }
}

/// A digest from its sharded segments `{alg}/{prefix}/{hash}`; `None` when the
/// shard does not open the hash or the key cannot belong to this angos version.
fn parse_sharded(algorithm: &str, prefix: &str, hash: &str) -> Option<Digest> {
    let digest = parse_digest(algorithm, hash)?;
    (hash.as_bytes().get(..2) == Some(prefix.as_bytes())).then_some(digest)
}

/// A digest from separate path segments; `None` means the key cannot belong
/// to this angos version.
fn parse_digest(algorithm: &str, hash: &str) -> Option<Digest> {
    let algorithm = Algorithm::from_str(algorithm).ok()?;
    Digest::with_algorithm(algorithm, hash).ok()
}

#[cfg(test)]
mod tests {
    use angos_oci::{Namespace, Tag};

    use crate::command::maintenance::categorize::*;
    use crate::{
        jobs::store::{LockKey, job_failed_path, job_lock_key_index_path, job_pending_path},
        registry::{
            keys::NamespaceKeys,
            metadata_store::{
                LinkKind,
                access_time::{atime_client_suffix, atime_entry_name},
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
            categorize(&digest_a().blob_path()),
            KeyCategory::BlobData { digest: digest_a() }
        );
    }

    #[test]
    fn blob_ref_paths_round_trip() {
        let links = [
            LinkKind::Blob(digest_a()),
            LinkKind::Digest(digest_a()),
            LinkKind::Layer(digest_a()),
            LinkKind::Config(digest_a()),
            LinkKind::Tag(Tag::new("v1.0").unwrap()),
            LinkKind::Referrer {
                subject: digest_b(),
                referrer: digest_a(),
            },
            LinkKind::Manifest {
                index: digest_b(),
                child: digest_a(),
            },
        ];
        for link in links {
            assert_eq!(
                categorize(&digest_a().blob_ref_path(&namespace(), &link)),
                KeyCategory::BlobRef {
                    digest: digest_a(),
                    namespace: "org/app".to_string(),
                    link: link.clone(),
                },
                "reference key for {link:?} must round-trip"
            );
        }
        assert_eq!(
            categorize(&digest_a().blob_ref_own_path(&namespace())),
            KeyCategory::BlobRef {
                digest: digest_a(),
                namespace: "org/app".to_string(),
                link: LinkKind::Blob(digest_a()),
            }
        );
    }

    #[test]
    fn tag_entry_and_atime_paths_round_trip() {
        let ns = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1.0").unwrap();
        let key = ns.tag_entry_path(&tag, u64::MAX - 1, false, &digest_a());
        assert_eq!(
            categorize(&key),
            KeyCategory::TagEntry {
                namespace: "org/app".to_string(),
                tag: "v1.0".to_string(),
            }
        );
    }

    #[test]
    fn atime_entry_paths_round_trip() {
        let ns = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1.0").unwrap();
        let name = atime_entry_name(u64::MAX - 1, &atime_client_suffix("alice"));
        assert_eq!(
            categorize(&format!("{}/{name}", ns.tag_atime_entry_dir(&tag))),
            KeyCategory::TagAtimeEntry {
                namespace: "org/app".to_string(),
                tag: "v1.0".to_string(),
            }
        );
        assert_eq!(
            categorize(&format!(
                "{}/{name}",
                ns.revision_atime_entry_dir(&digest_a())
            )),
            KeyCategory::RevisionAtimeEntry {
                namespace: "org/app".to_string(),
                digest: digest_a(),
            }
        );
        // The retired single keys are no shape this version knows.
        let unknown = [
            format!("v2/ns/org/app!atime/rev/sha256/{HASH_A}"),
            "v2/ns/org/app!atime/tag/v1.0".to_string(),
            format!("v2/ns/org/app!atime/tag/-bad!/{name}"),
            "v2/ns/org/app!atime/tag/v1.0!/junk".to_string(),
            format!("v2/ns/org/app!atime/rev/sha3/{HASH_A}!/{name}"),
            format!("v2/ns/org/app!atime/rev/sha256/{HASH_A}!/junk.entry"),
        ];
        for key in unknown {
            assert_eq!(categorize(&key), KeyCategory::Unknown, "key {key:?}");
        }
    }

    #[test]
    fn tag_hist_paths_round_trip() {
        let ns = Namespace::new("org/app").unwrap();
        let tag = Tag::new("v1.0").unwrap();
        let entry_key = ns.tag_entry_path(&tag, u64::MAX - 1, false, &digest_a());
        let file = entry_key.rsplit_once('/').unwrap().1;
        assert_eq!(
            categorize(&ns.tag_hist_path(&tag, file)),
            KeyCategory::TagHistory
        );
        assert_eq!(categorize("v2/ns/org/app!hist/v1.0"), KeyCategory::Unknown);
    }

    #[test]
    fn adversarial_tag_keys_are_unknown() {
        let unknown = [
            "v2/ns/org/app".to_string(),
            "v2/ns/org/app!tag/v1.0".to_string(),
            format!("v2/ns/org/app!tag/-bad!/{:016x}.set.sha256.{HASH_A}", 1),
            format!("v2/ns/org/app!tag/v1!/{:016x}.mov.sha256.{HASH_A}", 1),
            format!("v2/ns/BAD!tag/v1!/{:016x}.set.sha256.{HASH_A}", 1),
            "v2/ns/org/app!atime/tag/-bad".to_string(),
            "v2/ns/org/app!other/x".to_string(),
        ];
        for key in unknown {
            assert_eq!(categorize(&key), KeyCategory::Unknown, "key {key:?}");
        }
    }

    #[test]
    fn adversarial_blob_ref_keys_are_unknown() {
        let unknown = [
            format!("v2/ref/sha256/aa/{HASH_A}/ns!r/unknown"),
            format!("v2/ref/sha256/bb/{HASH_A}/ns!own"),
            format!("v2/ref/sha256/aa/{HASH_A}/ns"),
            format!("v2/ref/sha3/aa/{HASH_A}/ns!own"),
            "v2/ref/sha256/aa".to_string(),
        ];
        for key in unknown {
            assert_eq!(categorize(&key), KeyCategory::Unknown, "key {key:?}");
        }
    }

    /// The retired link-file subtrees are no layout this version knows, so they
    /// quarantine like any other unrecognized key rather than being deleted.
    #[test]
    fn retired_link_subtrees_are_unknown_keys() {
        let ns = namespace();
        for key in [
            format!("v2/repositories/{ns}/_manifests/tags/v1.0/current/link"),
            format!("v2/repositories/{ns}/_manifests/revisions/sha256/{HASH_A}/link"),
            format!(
                "v2/repositories/{ns}/_manifests/referrers/sha256/{HASH_A}/sha256/{HASH_B}/link"
            ),
            format!("v2/repositories/{ns}/_manifests/index/sha256/{HASH_A}/sha256/{HASH_B}/link"),
            format!("v2/repositories/{ns}/_blobs/sha256/{HASH_A}/link"),
            format!("v2/repositories/{ns}/_layers/sha256/{HASH_A}/link"),
            format!("v2/repositories/{ns}/_config/sha256/{HASH_A}/link"),
        ] {
            assert_eq!(categorize(&key), KeyCategory::Unknown, "key {key:?}");
        }
    }

    const SESSION: &str = "067e6162-3b6f-4ae2-a171-2470b63dff00";

    fn session() -> UploadSessionId {
        UploadSessionId::from_str(SESSION).unwrap()
    }

    #[test]
    fn upload_artifacts_round_trip() {
        let ns = namespace();
        let cases = [
            (ns.upload_path(&session()), UploadArtifact::Data),
            (
                ns.upload_session_path(&session()),
                UploadArtifact::SessionJson,
            ),
            (
                format!("v2/repositories/org/app/_uploads/{SESSION}/staged/7"),
                UploadArtifact::Staged,
            ),
        ];
        for (key, expected) in cases {
            assert_eq!(
                categorize(&key),
                KeyCategory::UploadArtifact {
                    namespace: "org/app".to_string(),
                    artifact: expected,
                },
                "upload artifact {key} must round-trip"
            );
        }
    }

    /// A directory angos never opened is not an upload session: it must reach
    /// the unknown-key quarantine instead of being reported as one.
    #[test]
    fn an_upload_directory_that_is_not_a_session_is_unknown() {
        let ns = namespace();
        for name in ["uuid-1", "", "..", "not-a-uuid", "067e6162"] {
            let key = format!("v2/repositories/org/app/_uploads/{name}/data");
            assert_eq!(
                categorize(&key),
                KeyCategory::Unknown,
                "'{name}' must not categorize as an upload session"
            );
        }
        assert_eq!(
            categorize(&ns.upload_path(&session())),
            KeyCategory::UploadArtifact {
                namespace: "org/app".to_string(),
                artifact: UploadArtifact::Data,
            }
        );
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
            categorize(&job_lock_key_index_path(
                "cache",
                &LockKey::new("a/b:c").expect("lock key")
            )),
            KeyCategory::JobIndex {
                queue: Queue::Cache,
            }
        );
    }

    #[test]
    fn retired_engine_keys_are_unknown_and_reserved_prefixes_are_recognized() {
        assert_eq!(categorize(".tx-log/0000-uuid.json"), KeyCategory::Unknown);
        assert_eq!(categorize(".tx-bodies/uuid/0"), KeyCategory::Unknown);
        assert_eq!(categorize(".tx-locks/aa/some-key"), KeyCategory::Unknown);
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
    fn layer_listings_are_recognized_and_other_layer_keys_are_not() {
        let digest = Digest::sha256_of_bytes(b"layer");
        for key in [digest.layer_entries_path(), digest.layer_checkpoints_path()] {
            assert_eq!(
                categorize(&key),
                KeyCategory::LayerListing {
                    digest: digest.clone()
                }
            );
        }
        assert_eq!(
            categorize(&format!("{}/other", digest.layer_dir())),
            KeyCategory::Unknown
        );
        assert_eq!(
            categorize("v2/layers/sha256/ab/short/entries"),
            KeyCategory::Unknown
        );
    }

    #[test]
    fn gc_markers_are_recognized_and_nested_gc_keys_are_not() {
        assert!(matches!(
            categorize("v2/gc/0f7a2f2e-run"),
            KeyCategory::GcMarker
        ));
        assert!(matches!(
            categorize("v2/gc/nested/key"),
            KeyCategory::Unknown
        ));
    }
}
