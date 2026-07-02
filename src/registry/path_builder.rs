use crate::{
    oci::{Digest, Namespace},
    registry::metadata_store::LinkKind,
};

const BLOBS_ROOT: &str = "v2/blobs";
const REPOS_ROOT: &str = "v2/repositories";
const JOBS_ROOT: &str = "_jobs";

/// The subtree markers that terminate the variable-length namespace in a
/// `v2/repositories/<ns>/...` key: the namespace precedes the first of these,
/// and holding any of them as a child marks a path as a namespace.
pub const NAMESPACE_MARKERS: [&str; 5] =
    ["_manifests", "_layers", "_config", "_blobs", "_uploads"];

pub fn blobs_root_dir() -> &'static str {
    BLOBS_ROOT
}

pub fn repository_dir() -> &'static str {
    REPOS_ROOT
}

pub fn jobs_root_dir() -> &'static str {
    JOBS_ROOT
}

/// Storage prefix for a namespace's repository subtree by its raw on-disk name,
/// so scrub can reclaim a directory whose name fails `Namespace` validation.
/// Returns `None` for an empty, `.`, or `..` segment that could escape the root.
pub fn namespace_dir(name: &str) -> Option<String> {
    if name.is_empty()
        || name
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return None;
    }
    Some(format!("{REPOS_ROOT}/{name}"))
}

fn blob_dir(digest: &Digest) -> String {
    format!(
        "{BLOBS_ROOT}/{}/{}/{}",
        digest.algorithm(),
        digest.hash_prefix(),
        digest.hash()
    )
}

pub fn blob_path(digest: &Digest) -> String {
    format!("{}/data", blob_dir(digest))
}

pub fn blob_index_path(digest: &Digest) -> String {
    format!("{}/index.json", blob_dir(digest))
}

pub fn blob_index_refs_dir(digest: &Digest) -> String {
    format!("{}/refs", blob_dir(digest))
}

pub fn blob_index_shard_path(digest: &Digest, namespace: &Namespace) -> String {
    // Percent-encode '/' and '%' so the filename is unambiguous.
    let safe_ns = namespace.replace('%', "%25").replace('/', "%2F");
    format!("{}/refs/{safe_ns}.json", blob_dir(digest))
}

pub fn blob_container_dir(digest: &Digest) -> String {
    blob_dir(digest)
}

/// Root directory holding every upload container for a namespace. Used to
/// enumerate the namespace's active sessions (one child directory per UUID).
pub fn uploads_root_dir(namespace: &Namespace) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads")
}

pub fn upload_container_path(namespace: &Namespace, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}")
}

pub fn upload_path(namespace: &Namespace, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/data")
}

/// Directory holding an upload's hasher-state checkpoints under `algorithm`, one
/// file per offset.
pub fn upload_hash_context_dir(namespace: &Namespace, uuid: &str, algorithm: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/hashstates/{algorithm}")
}

/// An upload's serialised hasher-state checkpoint under `algorithm` after
/// consuming bytes up to `offset`, allowing hash resumption after a crash.
pub fn upload_hash_context_path(
    namespace: &Namespace,
    uuid: &str,
    algorithm: &str,
    offset: u64,
) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/hashstates/{algorithm}/{offset}")
}

/// RFC3339 timestamp marking when the upload session was created. Used for
/// age-based orphan detection during scrub.
pub fn upload_start_date_path(namespace: &Namespace, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/startedat")
}

pub fn manifest_revisions_link_root_dir(namespace: &Namespace, algorithm: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/revisions/{algorithm}")
}

/// Root directory of a namespace's layer links for one algorithm
/// (`_layers/<algorithm>/<hash>/link`); its children are the layer hashes.
pub fn layers_link_root_dir(namespace: &Namespace, algorithm: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_layers/{algorithm}")
}

/// Root directory of a namespace's config links for one algorithm
/// (`_config/<algorithm>/<hash>/link`); its children are the config hashes.
pub fn config_link_root_dir(namespace: &Namespace, algorithm: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_config/{algorithm}")
}

pub fn manifest_tags_dir(namespace: &Namespace) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/tags")
}

/// Directory holding a single tag's `current/link`. Scrub uses this to remove a
/// tag directory whose name is invalid (and so cannot form a `LinkKind::Tag`).
pub fn manifest_tag_dir(namespace: &Namespace, tag: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/tags/{tag}")
}

pub fn manifest_referrers_dir(namespace: &Namespace, subject: &Digest) -> String {
    format!(
        "{REPOS_ROOT}/{namespace}/_manifests/referrers/{}/{}",
        subject.algorithm(),
        subject.hash()
    )
}

/// Root directory holding one child directory per pending queue
/// (`_jobs/pending/<queue>`).
pub fn job_pending_root_dir() -> String {
    format!("{JOBS_ROOT}/pending")
}

pub fn job_pending_dir(queue: &str) -> String {
    format!("{JOBS_ROOT}/pending/{queue}")
}

pub fn job_pending_path(queue: &str, id: &str) -> String {
    format!("{JOBS_ROOT}/pending/{queue}/{id}.json")
}

/// Root directory holding one child directory per dead-letter queue
/// (`_jobs/failed/<queue>`).
pub fn job_failed_root_dir() -> String {
    format!("{JOBS_ROOT}/failed")
}

pub fn job_failed_dir(queue: &str) -> String {
    format!("{JOBS_ROOT}/failed/{queue}")
}

pub fn job_failed_path(queue: &str, id: &str) -> String {
    format!("{JOBS_ROOT}/failed/{queue}/{id}.json")
}

/// Root directory holding one child directory per dedup-index queue
/// (`_jobs/index/<queue>`).
pub fn job_index_root_dir() -> String {
    format!("{JOBS_ROOT}/index")
}

/// Directory holding the per-`lock_key` dedup index files for one `queue`
/// (`_jobs/index/<queue>/<encoded-lock-key>.json`).
pub fn job_lock_key_index_dir(queue: &str) -> String {
    format!("{JOBS_ROOT}/index/{queue}")
}

/// Path to the `lock_key` to `storage_key` dedup index file, read by
/// `find_pending_with_lock_key` for an O(1) lookup.
pub fn job_lock_key_index_path(queue: &str, lock_key: &str) -> String {
    format!(
        "{JOBS_ROOT}/index/{queue}/{}.json",
        encode_job_lock_key(lock_key)
    )
}

/// Percent-encode characters unsafe as a filename or S3 key component so a
/// `lock_key` lands on the same path regardless of backend.
fn encode_job_lock_key(lock_key: &str) -> String {
    lock_key
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => {
                format!("%{:02X}", c as u32)
            }
            c => c.to_string(),
        })
        .collect()
}

pub fn link_path(link: &LinkKind, namespace: &Namespace) -> String {
    format!("{}/link", link_container_path(link, namespace))
}

pub fn link_container_path(link: &LinkKind, namespace: &Namespace) -> String {
    match link {
        LinkKind::Blob(digest) => {
            format!(
                "{REPOS_ROOT}/{namespace}/_blobs/{}/{}",
                digest.algorithm(),
                digest.hash()
            )
        }
        LinkKind::Tag(tag) => {
            format!("{REPOS_ROOT}/{namespace}/_manifests/tags/{tag}/current")
        }
        LinkKind::Digest(digest) => {
            format!(
                "{REPOS_ROOT}/{namespace}/_manifests/revisions/{}/{}",
                digest.algorithm(),
                digest.hash()
            )
        }
        LinkKind::Layer(digest) => {
            format!(
                "{REPOS_ROOT}/{namespace}/_layers/{}/{}",
                digest.algorithm(),
                digest.hash()
            )
        }
        LinkKind::Config(digest) => {
            format!(
                "{REPOS_ROOT}/{namespace}/_config/{}/{}",
                digest.algorithm(),
                digest.hash()
            )
        }
        LinkKind::Referrer(subject, referrer) => {
            format!(
                "{REPOS_ROOT}/{namespace}/_manifests/referrers/{}/{}/{}/{}",
                subject.algorithm(),
                subject.hash(),
                referrer.algorithm(),
                referrer.hash()
            )
        }
        LinkKind::Manifest(index, child) => {
            format!(
                "{REPOS_ROOT}/{namespace}/_manifests/index/{}/{}/{}/{}",
                index.algorithm(),
                index.hash(),
                child.algorithm(),
                child.hash()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oci::Tag;

    // Valid 64-char lowercase-hex sha256 hashes (the only shape `Digest` accepts).
    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    // Valid 128-char lowercase-hex sha512 hash.
    const HASH_512: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn test_blob_paths() {
        let digest = Digest::sha256(HASH_A).unwrap();
        assert_eq!(
            blob_path(&digest),
            format!("v2/blobs/sha256/aa/{HASH_A}/data")
        );
        assert_eq!(
            blob_index_path(&digest),
            format!("v2/blobs/sha256/aa/{HASH_A}/index.json")
        );
        assert_eq!(
            blob_container_dir(&digest),
            format!("v2/blobs/sha256/aa/{HASH_A}")
        );
    }

    #[test]
    fn test_blob_paths_sha512() {
        let digest = Digest::sha512(HASH_512).unwrap();
        assert_eq!(
            blob_path(&digest),
            format!("v2/blobs/sha512/cc/{HASH_512}/data")
        );
        assert_eq!(
            blob_index_path(&digest),
            format!("v2/blobs/sha512/cc/{HASH_512}/index.json")
        );
    }

    #[test]
    fn test_upload_paths() {
        let ns = Namespace::new("ns").unwrap();
        assert_eq!(
            upload_container_path(&ns, "uuid"),
            "v2/repositories/ns/_uploads/uuid"
        );
        assert_eq!(
            upload_path(&ns, "uuid"),
            "v2/repositories/ns/_uploads/uuid/data"
        );
        assert_eq!(uploads_root_dir(&ns), "v2/repositories/ns/_uploads");
        assert_eq!(
            upload_hash_context_path(&ns, "uuid", "sha256", 42),
            "v2/repositories/ns/_uploads/uuid/hashstates/sha256/42"
        );
        assert_eq!(
            upload_start_date_path(&ns, "uuid"),
            "v2/repositories/ns/_uploads/uuid/startedat"
        );
    }

    #[test]
    fn test_namespace_dir() {
        assert_eq!(namespace_dir("ns").unwrap(), "v2/repositories/ns");
        assert_eq!(namespace_dir("org/app").unwrap(), "v2/repositories/org/app");
        // Uppercase fails `Namespace` validation but is safe as a directory name.
        assert_eq!(namespace_dir("BadNS").unwrap(), "v2/repositories/BadNS");
        // Empty, traversal, and empty-segment names are rejected.
        for unsafe_name in ["", "..", ".", "a/../b", "a//b", "/a", "a/", "a/."] {
            assert!(
                namespace_dir(unsafe_name).is_none(),
                "'{unsafe_name}' must be rejected"
            );
        }
    }

    #[test]
    fn test_manifest_paths() {
        let ns = Namespace::new("ns").unwrap();
        assert_eq!(
            manifest_revisions_link_root_dir(&ns, "sha256"),
            "v2/repositories/ns/_manifests/revisions/sha256"
        );
        assert_eq!(manifest_tags_dir(&ns), "v2/repositories/ns/_manifests/tags");
        assert_eq!(
            manifest_tag_dir(&ns, "v1.0"),
            "v2/repositories/ns/_manifests/tags/v1.0"
        );

        let subject = Digest::sha256(HASH_A).unwrap();
        assert_eq!(
            manifest_referrers_dir(&ns, &subject),
            format!("v2/repositories/ns/_manifests/referrers/sha256/{HASH_A}")
        );
    }

    #[test]
    fn test_link_paths() {
        let ns = Namespace::new("ns").unwrap();
        let digest = Digest::sha256(HASH_A).unwrap();

        let blob = LinkKind::Blob(digest.clone());
        assert_eq!(
            link_path(&blob, &ns),
            format!("v2/repositories/ns/_blobs/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&blob, &ns),
            format!("v2/repositories/ns/_blobs/sha256/{HASH_A}")
        );

        let tag = LinkKind::Tag(Tag::new("v1.0").unwrap());
        assert_eq!(
            link_path(&tag, &ns),
            "v2/repositories/ns/_manifests/tags/v1.0/current/link"
        );
        assert_eq!(
            link_container_path(&tag, &ns),
            "v2/repositories/ns/_manifests/tags/v1.0/current"
        );

        let revision = LinkKind::Digest(digest.clone());
        assert_eq!(
            link_path(&revision, &ns),
            format!("v2/repositories/ns/_manifests/revisions/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&revision, &ns),
            format!("v2/repositories/ns/_manifests/revisions/sha256/{HASH_A}")
        );

        let layer = LinkKind::Layer(digest.clone());
        assert_eq!(
            link_path(&layer, &ns),
            format!("v2/repositories/ns/_layers/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&layer, &ns),
            format!("v2/repositories/ns/_layers/sha256/{HASH_A}")
        );

        let config = LinkKind::Config(digest.clone());
        assert_eq!(
            link_path(&config, &ns),
            format!("v2/repositories/ns/_config/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&config, &ns),
            format!("v2/repositories/ns/_config/sha256/{HASH_A}")
        );

        let subject = Digest::sha256(HASH_A).unwrap();
        let referrer = Digest::sha256(HASH_B).unwrap();
        let referrer_link = LinkKind::Referrer(subject, referrer);
        assert_eq!(
            link_path(&referrer_link, &ns),
            format!("v2/repositories/ns/_manifests/referrers/sha256/{HASH_A}/sha256/{HASH_B}/link")
        );
        assert_eq!(
            link_container_path(&referrer_link, &ns),
            format!("v2/repositories/ns/_manifests/referrers/sha256/{HASH_A}/sha256/{HASH_B}")
        );

        let index = Digest::sha256(HASH_A).unwrap();
        let child = Digest::sha256(HASH_B).unwrap();
        let manifest_link = LinkKind::Manifest(index, child);
        assert_eq!(
            link_path(&manifest_link, &ns),
            format!("v2/repositories/ns/_manifests/index/sha256/{HASH_A}/sha256/{HASH_B}/link")
        );
        assert_eq!(
            link_container_path(&manifest_link, &ns),
            format!("v2/repositories/ns/_manifests/index/sha256/{HASH_A}/sha256/{HASH_B}")
        );
    }

    #[test]
    fn test_job_paths() {
        assert_eq!(job_pending_root_dir(), "_jobs/pending");
        assert_eq!(job_pending_dir("cache"), "_jobs/pending/cache");
        assert_eq!(
            job_pending_path("cache", "01HABCDE"),
            "_jobs/pending/cache/01HABCDE.json"
        );
        assert_eq!(job_failed_root_dir(), "_jobs/failed");
        assert_eq!(
            job_failed_path("cache", "01HABCDE"),
            "_jobs/failed/cache/01HABCDE.json"
        );
        assert_eq!(job_lock_key_index_dir("cache"), "_jobs/index/cache");
        assert_eq!(
            job_lock_key_index_path("cache", "cache.ns:sha256:abc"),
            "_jobs/index/cache/cache.ns%3Asha256%3Aabc.json"
        );
    }
}
