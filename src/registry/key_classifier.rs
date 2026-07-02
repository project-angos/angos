//! Inverse of every storage-key minter.
//!
//! [`classify_key`] routes a raw storage key to the domain that minted it,
//! recognizes engine/sibling prefixes scrub never owns, or reports a typed
//! reason why it matches no minter. Pure string logic: total and panic-free.

use std::str::FromStr;

use crate::{
    command::scrub::sweep_sink::SCRUB_AUDIT_PREFIX,
    oci::{Algorithm, Digest},
    registry::{
        job_store::Queue,
        path_builder::{self, NAMESPACE_MARKERS},
    },
};

/// The classification of one raw storage key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyClass {
    /// Minted by one of the registry's owned domains.
    Owned(KeyDomain),
    /// A recognized engine or sibling prefix scrub never sweeps.
    OutOfDomain(SiblingPrefix),
    /// Matches no minter; carries the typed reason.
    Unrecognized(UnrecognizedReason),
}

/// Which owned domain minted a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyDomain {
    /// `v2/blobs/<algo>/<prefix>/<hash>/data`.
    BlobBytes,
    /// `v2/blobs/<algo>/<prefix>/<hash>/index.json` and `.../refs/<ns>.json`.
    BlobIndexGrant,
    /// `v2/repositories/<ns>/{_manifests,_layers,_config,_blobs}/...` link shapes.
    MetadataLink,
    /// `v2/repositories/<ns>/_uploads/<uuid>/...` session objects.
    Upload,
    /// `_jobs/{pending,failed}/<queue>/<id>.json` and
    /// `_jobs/index/<queue>/<encoded>.json`.
    Job,
}

/// A bucket-root prefix owned by the transaction engine or by scrub, recognized
/// so it is never treated as unrecognized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiblingPrefix {
    TxLocks,
    TxLog,
    TxBodies,
    /// Dead pre-1.3 namespace-registry prefix.
    Registry,
    /// The `_scrub-audit/` prefix carrying the scrub run marker.
    ScrubAudit,
}

/// Why a key matched no minter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnrecognizedReason {
    /// A blob or link algorithm segment is not `sha256`/`sha512`.
    UnknownAlgorithm,
    /// A job queue segment is not a recognized [`Queue`].
    UnknownQueue,
    /// Under a valid `_uploads/<uuid>/`, the tail is not a recognized upload shape.
    UnknownStagingShape,
    /// Well-prefixed but a segment is empty, a hash fails digest validation, an
    /// offset is not a `u64`, or a namespace segment is `.`/`..`/empty.
    UnparseableSegment,
    /// The prefix matches no owned root and no sibling whitelist.
    NoMinterMatch,
}

impl UnrecognizedReason {
    /// Stable label for tallies and log lines, the single home for these names.
    pub fn label(self) -> &'static str {
        match self {
            UnrecognizedReason::UnknownAlgorithm => "unknown-algorithm",
            UnrecognizedReason::UnknownQueue => "unknown-queue",
            UnrecognizedReason::UnknownStagingShape => "unknown-staging-shape",
            UnrecognizedReason::UnparseableSegment => "unparseable-segment",
            UnrecognizedReason::NoMinterMatch => "no-minter-match",
        }
    }
}

// Transaction-engine sibling roots. The engine mints these in
// `angos_tx_engine::intent` and `angos_tx_engine::lock::storage::s3`; the
// cross-check test below pins the literals against those minters.
const TX_LOCKS_PREFIX: &str = ".tx-locks/";
const TX_LOG_PREFIX: &str = ".tx-log/";
const TX_BODIES_PREFIX: &str = ".tx-bodies/";
const REGISTRY_PREFIX: &str = "_registry/";

/// Classify one raw storage key against every minter. Total and panic-free.
pub fn classify_key(key: &str) -> KeyClass {
    if let Some(sibling) = sibling_prefix(key) {
        return KeyClass::OutOfDomain(sibling);
    }
    if let Some(rest) = strip_root(key, path_builder::blobs_root_dir()) {
        return classify_blob(rest);
    }
    if let Some(rest) = strip_root(key, path_builder::repository_dir()) {
        return classify_repository(rest);
    }
    if let Some(class) = classify_job(key) {
        return class;
    }
    KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch)
}

/// Match a recognized engine/sibling bucket-root prefix.
fn sibling_prefix(key: &str) -> Option<SiblingPrefix> {
    if key.starts_with(TX_LOCKS_PREFIX) {
        Some(SiblingPrefix::TxLocks)
    } else if key.starts_with(TX_LOG_PREFIX) {
        Some(SiblingPrefix::TxLog)
    } else if key.starts_with(TX_BODIES_PREFIX) {
        Some(SiblingPrefix::TxBodies)
    } else if key.starts_with(REGISTRY_PREFIX) {
        Some(SiblingPrefix::Registry)
    } else if strip_root(key, SCRUB_AUDIT_PREFIX).is_some() {
        Some(SiblingPrefix::ScrubAudit)
    } else {
        None
    }
}

/// Strip `root` plus its trailing slash, returning the remainder when `key` is
/// under that root.
fn strip_root<'a>(key: &'a str, root: &str) -> Option<&'a str> {
    key.strip_prefix(root)?.strip_prefix('/')
}

/// Validate the leading `<algo>/<prefix>/<hash>` of a blob path, returning the
/// addressed digest and the tail after the hash segment when the container is
/// well-formed.
fn parse_blob_container(rest: &str) -> Result<(Digest, &str), UnrecognizedReason> {
    let mut parts = rest.splitn(4, '/');
    let algo = parts.next().filter(|s| !s.is_empty());
    let prefix = parts.next();
    let hash = parts.next();
    let tail = parts.next().unwrap_or("");

    let (Some(algo), Some(prefix), Some(hash)) = (algo, prefix, hash) else {
        return Err(UnrecognizedReason::UnparseableSegment);
    };
    let Ok(algorithm) = Algorithm::from_str(algo) else {
        return Err(UnrecognizedReason::UnknownAlgorithm);
    };
    let Ok(digest) = Digest::with_algorithm(algorithm, hash) else {
        return Err(UnrecognizedReason::UnparseableSegment);
    };
    if prefix != digest.hash_prefix() {
        return Err(UnrecognizedReason::UnparseableSegment);
    }
    Ok((digest, tail))
}

/// Recover the digest addressed by a `v2/blobs/**` key whose tail is exactly
/// `expected_tail`, or `None` for any other shape.
fn blob_container_digest(key: &str, expected_tail: &str) -> Option<Digest> {
    let rest = strip_root(key, path_builder::blobs_root_dir())?;
    match parse_blob_container(rest) {
        Ok((digest, tail)) if tail == expected_tail => Some(digest),
        _ => None,
    }
}

/// Recover the [`Digest`] addressed by a `v2/blobs/<algo>/<prefix>/<hash>/data`
/// key, or `None` for any key that is not that exact byte-data shape. A key
/// classifying [`KeyDomain::BlobBytes`] always yields a digest; index/refs grant
/// keys return `None`.
pub fn blob_digest_from_key(key: &str) -> Option<Digest> {
    blob_container_digest(key, "data")
}

/// Recover the [`Digest`] addressed by a legacy
/// `v2/blobs/<algo>/<prefix>/<hash>/index.json` key, or `None` for any other
/// shape (`refs/` shards and data keys included).
pub fn legacy_index_digest_from_key(key: &str) -> Option<Digest> {
    blob_container_digest(key, "index.json")
}

/// Classify a key under `v2/blobs/`.
fn classify_blob(rest: &str) -> KeyClass {
    let tail = match parse_blob_container(rest) {
        Ok((_, tail)) => tail,
        Err(reason) => return KeyClass::Unrecognized(reason),
    };
    match tail {
        "data" => KeyClass::Owned(KeyDomain::BlobBytes),
        "index.json" => KeyClass::Owned(KeyDomain::BlobIndexGrant),
        other => match other.strip_prefix("refs/") {
            Some(shard) if is_blob_index_shard(shard) => KeyClass::Owned(KeyDomain::BlobIndexGrant),
            Some(_) => KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment),
            None => KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch),
        },
    }
}

/// A blob-index shard filename is a single non-empty `<safe_ns>.json` segment.
fn is_blob_index_shard(shard: &str) -> bool {
    match shard.strip_suffix(".json") {
        Some(stem) => !stem.is_empty() && !stem.contains('/'),
        None => false,
    }
}

/// Classify a key under `v2/repositories/`.
fn classify_repository(rest: &str) -> KeyClass {
    let Some((namespace, marker, tail)) = split_namespace(rest) else {
        return KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch);
    };
    if !namespace_segments_valid(namespace) {
        return KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment);
    }
    match marker {
        "_uploads" => classify_upload(tail),
        "_manifests" => classify_manifests(tail),
        "_layers" | "_config" | "_blobs" => classify_digest_link(tail),
        _ => KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch),
    }
}

/// Split `<ns>/<marker>/<tail>` at the first structural marker. Returns the
/// namespace, the marker segment, and the remaining tail (possibly empty).
fn split_namespace(rest: &str) -> Option<(&str, &str, &str)> {
    let mut search_from = 0;
    while let Some(slash) = rest[search_from..].find('/') {
        let marker_start = search_from + slash + 1;
        let segment_end = rest[marker_start..]
            .find('/')
            .map_or(rest.len(), |i| marker_start + i);
        let segment = &rest[marker_start..segment_end];
        if NAMESPACE_MARKERS.contains(&segment) {
            let namespace = &rest[..marker_start - 1];
            let tail = rest.get(segment_end + 1..).unwrap_or("");
            return Some((namespace, segment, tail));
        }
        search_from = marker_start;
    }
    None
}

/// Every namespace segment is non-empty and not `.`/`..`.
fn namespace_segments_valid(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// Classify the tail under `_uploads/`: `<uuid>/<rest>`.
fn classify_upload(tail: &str) -> KeyClass {
    let Some((uuid, rest)) = tail.split_once('/') else {
        return KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment);
    };
    if uuid.is_empty() {
        return KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment);
    }
    match rest {
        "data" | "startedat" | "staged/coalesce" => KeyClass::Owned(KeyDomain::Upload),
        _ => classify_upload_tail(rest),
    }
}

/// Classify the multi-segment upload tails: `hashstates/<algo>/<offset>` and
/// `staged/<offset>`.
fn classify_upload_tail(rest: &str) -> KeyClass {
    if let Some(hashstate) = rest.strip_prefix("hashstates/") {
        let Some((algo, offset)) = hashstate.split_once('/') else {
            return KeyClass::Unrecognized(UnrecognizedReason::UnknownStagingShape);
        };
        if Algorithm::from_str(algo).is_err() {
            return KeyClass::Unrecognized(UnrecognizedReason::UnknownAlgorithm);
        }
        return offset_class(offset);
    }
    if let Some(offset) = rest.strip_prefix("staged/") {
        return offset_class(offset);
    }
    KeyClass::Unrecognized(UnrecognizedReason::UnknownStagingShape)
}

/// An upload offset segment is a bare `u64` with no further path segments.
fn offset_class(offset: &str) -> KeyClass {
    if offset.contains('/') {
        return KeyClass::Unrecognized(UnrecognizedReason::UnknownStagingShape);
    }
    match offset.parse::<u64>() {
        Ok(_) => KeyClass::Owned(KeyDomain::Upload),
        Err(_) => KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment),
    }
}

/// Classify the tail under `_manifests/`: tags / revisions / referrers / index.
fn classify_manifests(tail: &str) -> KeyClass {
    if let Some(rest) = tail.strip_prefix("tags/") {
        return classify_tag_link(rest);
    }
    if let Some(rest) = tail.strip_prefix("revisions/") {
        return classify_digest_link(rest);
    }
    if let Some(rest) = tail.strip_prefix("referrers/") {
        return classify_digest_pair_link(rest);
    }
    if let Some(rest) = tail.strip_prefix("index/") {
        return classify_digest_pair_link(rest);
    }
    KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch)
}

/// `<tag>/current/link`: the tag is free-form, terminated by `/current/link`.
fn classify_tag_link(rest: &str) -> KeyClass {
    match rest.strip_suffix("/current/link") {
        Some(tag) if !tag.is_empty() && !tag.contains('/') => {
            KeyClass::Owned(KeyDomain::MetadataLink)
        }
        Some(_) => KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment),
        None => KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch),
    }
}

/// `<algo>/<hash>/link`: one digest then the link file.
fn classify_digest_link(rest: &str) -> KeyClass {
    let Some(body) = rest.strip_suffix("/link") else {
        return KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch);
    };
    match parse_digest_segments(body) {
        Ok("") => KeyClass::Owned(KeyDomain::MetadataLink),
        Ok(_) => KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch),
        Err(reason) => KeyClass::Unrecognized(reason),
    }
}

/// `<algo>/<hash>/<algo>/<hash>/link`: a digest pair then the link file.
fn classify_digest_pair_link(rest: &str) -> KeyClass {
    let Some(body) = rest.strip_suffix("/link") else {
        return KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch);
    };
    let remainder = match parse_digest_segments(body) {
        Ok(remainder) => remainder,
        Err(reason) => return KeyClass::Unrecognized(reason),
    };
    let Some(second) = remainder.strip_prefix('/') else {
        return KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch);
    };
    match parse_digest_segments(second) {
        Ok("") => KeyClass::Owned(KeyDomain::MetadataLink),
        Ok(_) => KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch),
        Err(reason) => KeyClass::Unrecognized(reason),
    }
}

/// Consume a leading `<algo>/<hash>` digest, returning the remainder (with no
/// leading slash stripped) when valid.
fn parse_digest_segments(body: &str) -> Result<&str, UnrecognizedReason> {
    let mut parts = body.splitn(3, '/');
    let algo = parts.next().filter(|s| !s.is_empty());
    let hash = parts.next();
    let (Some(algo), Some(hash)) = (algo, hash) else {
        return Err(UnrecognizedReason::UnparseableSegment);
    };
    let Ok(algorithm) = Algorithm::from_str(algo) else {
        return Err(UnrecognizedReason::UnknownAlgorithm);
    };
    if Digest::with_algorithm(algorithm, hash).is_err() {
        return Err(UnrecognizedReason::UnparseableSegment);
    }
    // Keep the leading slash so the caller distinguishes a second digest segment
    // from end-of-input.
    let consumed = algo.len() + 1 + hash.len();
    Ok(&body[consumed..])
}

/// Classify a key under the jobs root: pending / failed / index. Returns `None`
/// when the key is not under the jobs root, so the caller falls through to
/// `NoMinterMatch`.
fn classify_job(key: &str) -> Option<KeyClass> {
    let pending_root = path_builder::job_pending_root_dir();
    let failed_root = path_builder::job_failed_root_dir();
    let index_root = path_builder::job_index_root_dir();

    if let Some(body) = strip_root(key, &pending_root) {
        return Some(classify_job_body(body));
    }
    if let Some(body) = strip_root(key, &failed_root) {
        return Some(classify_job_body(body));
    }
    if let Some(body) = strip_root(key, &index_root) {
        return Some(classify_job_body(body));
    }
    None
}

/// `<queue>/<id>.json` under a job partition; an unknown queue reports
/// `UnknownQueue` even when the rest is well-formed.
fn classify_job_body(body: &str) -> KeyClass {
    let Some((queue, id)) = body.split_once('/') else {
        return KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment);
    };
    if Queue::from_str(queue).is_err() {
        return KeyClass::Unrecognized(UnrecognizedReason::UnknownQueue);
    }
    match id.strip_suffix(".json") {
        Some(stem) if !stem.is_empty() && !stem.contains('/') => KeyClass::Owned(KeyDomain::Job),
        Some(_) => KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment),
        None => KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch),
    }
}

#[cfg(test)]
mod tests {
    use angos_tx_engine::{intent::body_ref_key, lock::storage::s3::lock_path};
    use uuid::Uuid;

    use super::*;
    use crate::{
        oci::{Namespace, Tag},
        registry::{
            metadata_store::LinkKind,
            path_builder::{job_failed_path, job_lock_key_index_path, job_pending_path},
        },
    };

    const HASH_256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_256_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HASH_512: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn sha256() -> Digest {
        Digest::sha256(HASH_256).unwrap()
    }

    fn sha512() -> Digest {
        Digest::sha512(HASH_512).unwrap()
    }

    // -- Positive owned shapes --------------------------------------------

    #[test]
    fn blob_data_is_blob_bytes() {
        assert_eq!(
            classify_key(&path_builder::blob_path(&sha256())),
            KeyClass::Owned(KeyDomain::BlobBytes)
        );
        assert_eq!(
            classify_key(&path_builder::blob_path(&sha512())),
            KeyClass::Owned(KeyDomain::BlobBytes)
        );
    }

    #[test]
    fn blob_digest_from_key_round_trips_the_data_key() {
        for digest in [sha256(), sha512()] {
            let key = path_builder::blob_path(&digest);
            assert_eq!(
                blob_digest_from_key(&key),
                Some(digest.clone()),
                "the byte-data key must yield its digest"
            );
        }
        // Only the byte-data tail carries a reapable digest; grants do not.
        assert_eq!(
            blob_digest_from_key(&path_builder::blob_index_path(&sha256())),
            None
        );
        let nested = Namespace::new("org/app").unwrap();
        assert_eq!(
            blob_digest_from_key(&path_builder::blob_index_shard_path(&sha256(), &nested)),
            None
        );
        // A non-blob key never yields a digest.
        assert_eq!(blob_digest_from_key("totally/foreign/key"), None);
        // A junk hash that fails digest validation yields nothing.
        assert_eq!(blob_digest_from_key("v2/blobs/sha256/aa/abc/data"), None);
    }

    #[test]
    fn legacy_index_digest_from_key_matches_only_index_json() {
        for digest in [sha256(), sha512()] {
            assert_eq!(
                legacy_index_digest_from_key(&path_builder::blob_index_path(&digest)),
                Some(digest.clone()),
                "the legacy index key must yield its digest"
            );
            assert_eq!(
                legacy_index_digest_from_key(&path_builder::blob_path(&digest)),
                None
            );
        }
        let nested = Namespace::new("org/app").unwrap();
        assert_eq!(
            legacy_index_digest_from_key(&path_builder::blob_index_shard_path(&sha256(), &nested)),
            None
        );
    }

    #[test]
    fn blob_index_and_shard_are_grants() {
        assert_eq!(
            classify_key(&path_builder::blob_index_path(&sha256())),
            KeyClass::Owned(KeyDomain::BlobIndexGrant)
        );
        let nested = Namespace::new("org/app").unwrap();
        assert_eq!(
            classify_key(&path_builder::blob_index_shard_path(&sha256(), &nested)),
            KeyClass::Owned(KeyDomain::BlobIndexGrant)
        );
    }

    #[test]
    fn every_link_kind_is_metadata_link() {
        let ns = Namespace::new("nginx").unwrap();
        let links = [
            LinkKind::Blob(sha256()),
            LinkKind::Tag(Tag::new("v1.0").unwrap()),
            LinkKind::Digest(sha256()),
            LinkKind::Layer(sha256()),
            LinkKind::Config(sha256()),
            LinkKind::Referrer(sha256(), Digest::sha256(HASH_256_B).unwrap()),
            LinkKind::Manifest(sha256(), Digest::sha256(HASH_256_B).unwrap()),
        ];
        for link in links {
            assert_eq!(
                classify_key(&path_builder::link_path(&link, &ns)),
                KeyClass::Owned(KeyDomain::MetadataLink),
                "link {link:?} must classify as a metadata link"
            );
        }
    }

    #[test]
    fn upload_shapes_are_upload() {
        let ns = Namespace::new("nginx").unwrap();
        assert_eq!(
            classify_key(&path_builder::upload_path(&ns, "uuid")),
            KeyClass::Owned(KeyDomain::Upload)
        );
        assert_eq!(
            classify_key(&path_builder::upload_start_date_path(&ns, "uuid")),
            KeyClass::Owned(KeyDomain::Upload)
        );
        assert_eq!(
            classify_key(&path_builder::upload_hash_context_path(
                &ns, "uuid", "sha256", 42
            )),
            KeyClass::Owned(KeyDomain::Upload)
        );
        let container = path_builder::upload_container_path(&ns, "uuid");
        assert_eq!(
            classify_key(&format!("{container}/staged/0")),
            KeyClass::Owned(KeyDomain::Upload)
        );
        assert_eq!(
            classify_key(&format!("{container}/staged/coalesce")),
            KeyClass::Owned(KeyDomain::Upload)
        );
    }

    #[test]
    fn job_shapes_are_job() {
        for queue in [Queue::Cache, Queue::Replication] {
            assert_eq!(
                classify_key(&job_pending_path(queue.as_str(), "0000000000000000-id")),
                KeyClass::Owned(KeyDomain::Job)
            );
            assert_eq!(
                classify_key(&job_failed_path(queue.as_str(), "0000000000000000-id")),
                KeyClass::Owned(KeyDomain::Job)
            );
            assert_eq!(
                classify_key(&job_lock_key_index_path(queue.as_str(), "lock:key")),
                KeyClass::Owned(KeyDomain::Job)
            );
        }
    }

    #[test]
    fn nested_namespace_link_and_upload_are_owned() {
        let ns = Namespace::new("org/project/image").unwrap();
        assert_eq!(
            classify_key(&path_builder::link_path(&LinkKind::Layer(sha256()), &ns)),
            KeyClass::Owned(KeyDomain::MetadataLink)
        );
        assert_eq!(
            classify_key(&path_builder::upload_path(&ns, "uuid-here")),
            KeyClass::Owned(KeyDomain::Upload)
        );
    }

    // -- Siblings ----------------------------------------------------------

    #[test]
    fn sibling_prefixes_are_out_of_domain() {
        assert_eq!(
            classify_key(".tx-locks/x"),
            KeyClass::OutOfDomain(SiblingPrefix::TxLocks)
        );
        assert_eq!(
            classify_key(".tx-log/x"),
            KeyClass::OutOfDomain(SiblingPrefix::TxLog)
        );
        assert_eq!(
            classify_key(".tx-bodies/x"),
            KeyClass::OutOfDomain(SiblingPrefix::TxBodies)
        );
        assert_eq!(
            classify_key("_registry/x"),
            KeyClass::OutOfDomain(SiblingPrefix::Registry)
        );
        assert_eq!(
            classify_key("_scrub-audit/latest.json"),
            KeyClass::OutOfDomain(SiblingPrefix::ScrubAudit)
        );
    }

    /// The hardcoded tx-engine sibling literals match what the engine actually
    /// mints; a prefix change in the engine breaks here instead of silently
    /// turning engine keys unrecognized.
    #[test]
    fn tx_engine_minted_keys_classify_out_of_domain() {
        let id = Uuid::new_v4();
        assert_eq!(
            classify_key(&body_ref_key(id, 0)),
            KeyClass::OutOfDomain(SiblingPrefix::TxBodies)
        );
        assert_eq!(
            classify_key(&lock_path("blob-data:x")),
            KeyClass::OutOfDomain(SiblingPrefix::TxLocks)
        );
        // The intent log key shape (`.tx-log/<id>.json`, see `IntentRecord::log_key`).
        assert_eq!(
            classify_key(&format!(".tx-log/{id}.json")),
            KeyClass::OutOfDomain(SiblingPrefix::TxLog)
        );
    }

    // -- Typed reasons -----------------------------------------------------

    #[test]
    fn unknown_algorithm() {
        assert_eq!(
            classify_key(&format!("v2/blobs/md5/aa/{HASH_256}/data")),
            KeyClass::Unrecognized(UnrecognizedReason::UnknownAlgorithm)
        );
        assert_eq!(
            classify_key(&format!("v2/repositories/ns/_layers/md5/{HASH_256}/link")),
            KeyClass::Unrecognized(UnrecognizedReason::UnknownAlgorithm)
        );
    }

    #[test]
    fn unknown_queue() {
        assert_eq!(
            classify_key("_jobs/pending/bogus/x.json"),
            KeyClass::Unrecognized(UnrecognizedReason::UnknownQueue)
        );
        assert_eq!(
            classify_key("_jobs/index/bogus/x.json"),
            KeyClass::Unrecognized(UnrecognizedReason::UnknownQueue)
        );
    }

    #[test]
    fn unknown_staging_shape() {
        // A bare `staged` with no offset segment.
        assert_eq!(
            classify_key("v2/repositories/ns/_uploads/uuid/staged"),
            KeyClass::Unrecognized(UnrecognizedReason::UnknownStagingShape)
        );
        // `hashstates/<algo>` with no offset.
        assert_eq!(
            classify_key("v2/repositories/ns/_uploads/uuid/hashstates/sha256"),
            KeyClass::Unrecognized(UnrecognizedReason::UnknownStagingShape)
        );
        // A junk tail under a valid upload container.
        assert_eq!(
            classify_key("v2/repositories/ns/_uploads/uuid/junk"),
            KeyClass::Unrecognized(UnrecognizedReason::UnknownStagingShape)
        );
    }

    #[test]
    fn unparseable_segment() {
        // Hash too short for sha256.
        assert_eq!(
            classify_key("v2/blobs/sha256/aa/abc/data"),
            KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment)
        );
        // Prefix does not match the hash's first two chars.
        assert_eq!(
            classify_key(&format!("v2/blobs/sha256/zz/{HASH_256}/data")),
            KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment)
        );
        // A `..` namespace segment.
        assert_eq!(
            classify_key(&format!(
                "v2/repositories/../_layers/sha256/{HASH_256}/link"
            )),
            KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment)
        );
        // Non-numeric upload offset.
        assert_eq!(
            classify_key("v2/repositories/ns/_uploads/uuid/staged/notanumber"),
            KeyClass::Unrecognized(UnrecognizedReason::UnparseableSegment)
        );
    }

    #[test]
    fn no_minter_match() {
        assert_eq!(
            classify_key("totally/foreign/key"),
            KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch)
        );
        // A valid blob container with an unknown tail file.
        assert_eq!(
            classify_key(&format!("v2/blobs/sha256/aa/{HASH_256}/unknown")),
            KeyClass::Unrecognized(UnrecognizedReason::NoMinterMatch)
        );
    }

    /// Every minter output classifies non-unrecognized. Adding a minter shape or
    /// changing a `path_builder` shape without teaching the classifier breaks here.
    #[test]
    fn every_minter_output_classifies_non_unrecognized() {
        let namespaces = [
            Namespace::new("nginx").unwrap(),
            Namespace::new("org/app").unwrap(),
        ];
        let digests = [
            (
                Digest::sha256(HASH_256).unwrap(),
                Digest::sha256(HASH_256_B).unwrap(),
            ),
            (
                sha512(),
                Digest::sha512(HASH_512.replace('c', "d")).unwrap(),
            ),
        ];

        for (primary, secondary) in &digests {
            assert_eq!(
                classify_key(&path_builder::blob_path(primary)),
                KeyClass::Owned(KeyDomain::BlobBytes)
            );
            assert_eq!(
                classify_key(&path_builder::blob_index_path(primary)),
                KeyClass::Owned(KeyDomain::BlobIndexGrant)
            );

            for ns in &namespaces {
                assert_eq!(
                    classify_key(&path_builder::blob_index_shard_path(primary, ns)),
                    KeyClass::Owned(KeyDomain::BlobIndexGrant)
                );

                let links = [
                    LinkKind::Blob(primary.clone()),
                    LinkKind::Tag(Tag::new("v1").unwrap()),
                    LinkKind::Digest(primary.clone()),
                    LinkKind::Layer(primary.clone()),
                    LinkKind::Config(primary.clone()),
                    LinkKind::Referrer(primary.clone(), secondary.clone()),
                    LinkKind::Manifest(primary.clone(), secondary.clone()),
                ];
                for link in links {
                    assert_eq!(
                        classify_key(&path_builder::link_path(&link, ns)),
                        KeyClass::Owned(KeyDomain::MetadataLink),
                        "minter link {link:?} in {ns} must be owned"
                    );
                }

                assert_eq!(
                    classify_key(&path_builder::upload_path(ns, "uuid")),
                    KeyClass::Owned(KeyDomain::Upload)
                );
                assert_eq!(
                    classify_key(&path_builder::upload_start_date_path(ns, "uuid")),
                    KeyClass::Owned(KeyDomain::Upload)
                );
                assert_eq!(
                    classify_key(&path_builder::upload_hash_context_path(
                        ns,
                        "uuid",
                        primary.algorithm().as_str(),
                        42
                    )),
                    KeyClass::Owned(KeyDomain::Upload)
                );
                let container = path_builder::upload_container_path(ns, "uuid");
                assert_eq!(
                    classify_key(&format!("{container}/staged/0")),
                    KeyClass::Owned(KeyDomain::Upload)
                );
                assert_eq!(
                    classify_key(&format!("{container}/staged/coalesce")),
                    KeyClass::Owned(KeyDomain::Upload)
                );
            }
        }

        for queue in [Queue::Cache, Queue::Replication] {
            assert_eq!(
                classify_key(&job_pending_path(queue.as_str(), "0000000000000000-id")),
                KeyClass::Owned(KeyDomain::Job)
            );
            assert_eq!(
                classify_key(&job_failed_path(queue.as_str(), "0000000000000000-id")),
                KeyClass::Owned(KeyDomain::Job)
            );
            assert_eq!(
                classify_key(&job_lock_key_index_path(
                    queue.as_str(),
                    "cache.ns:sha256:abc"
                )),
                KeyClass::Owned(KeyDomain::Job)
            );
        }
    }
}
