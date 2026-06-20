use crate::{oci::Digest, registry::metadata_store::LinkKind};

const BLOBS_ROOT: &str = "v2/blobs";
const REPOS_ROOT: &str = "v2/repositories";
const JOBS_ROOT: &str = "_jobs";

pub fn blobs_root_dir() -> &'static str {
    BLOBS_ROOT
}

pub fn repository_dir() -> &'static str {
    REPOS_ROOT
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

pub fn blob_index_shard_path(digest: &Digest, namespace: &str) -> String {
    // Encode namespace as a safe filename: percent-encode '/' and '%' to avoid
    // ambiguity (namespaces can contain underscores, so '/' -> '_' is lossy).
    let safe_ns = namespace.replace('%', "%25").replace('/', "%2F");
    format!("{}/refs/{safe_ns}.json", blob_dir(digest))
}

pub fn blob_container_dir(digest: &Digest) -> String {
    blob_dir(digest)
}

/// Root directory holding every upload container for a namespace. Used to
/// enumerate the namespace's active sessions (one child directory per UUID).
pub fn uploads_root_dir(namespace: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads")
}

pub fn upload_container_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}")
}

pub fn upload_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/data")
}

/// Directory holding an upload's hasher-state checkpoints under `algorithm`, one
/// file per offset. Used to enumerate checkpoints and pick the most recent.
pub fn upload_hash_context_dir(namespace: &str, uuid: &str, algorithm: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/hashstates/{algorithm}")
}

/// An upload's serialised hasher-state checkpoint under `algorithm` after
/// consuming its bytes up to `offset`. One file per offset, allowing hash
/// resumption after a crash without re-reading the uploaded bytes.
pub fn upload_hash_context_path(
    namespace: &str,
    uuid: &str,
    algorithm: &str,
    offset: u64,
) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/hashstates/{algorithm}/{offset}")
}

/// RFC3339 timestamp marking when the upload session was created. Used for
/// age-based orphan detection during scrub.
pub fn upload_start_date_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/startedat")
}

pub fn manifest_revisions_link_root_dir(namespace: &str, algorithm: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/revisions/{algorithm}")
}

pub fn manifest_tags_dir(namespace: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/tags")
}

/// Directory holding a single tag's `current/link`. Scrub uses this to remove a
/// tag directory whose name is invalid (and so cannot form a `LinkKind::Tag`).
pub fn manifest_tag_dir(namespace: &str, tag: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/tags/{tag}")
}

pub fn manifest_referrers_dir(namespace: &str, subject: &Digest) -> String {
    format!(
        "{REPOS_ROOT}/{namespace}/_manifests/referrers/{}/{}",
        subject.algorithm(),
        subject.hash()
    )
}

pub fn job_pending_dir(queue: &str) -> String {
    format!("{JOBS_ROOT}/pending/{queue}")
}

pub fn job_pending_path(queue: &str, id: &str) -> String {
    format!("{JOBS_ROOT}/pending/{queue}/{id}.json")
}

pub fn job_failed_dir(queue: &str) -> String {
    format!("{JOBS_ROOT}/failed/{queue}")
}

pub fn job_failed_path(queue: &str, id: &str) -> String {
    format!("{JOBS_ROOT}/failed/{queue}/{id}.json")
}

/// Path to the `lock_key` → `storage_key` dedup index file. Each pending
/// envelope has at most one such file alongside it; `find_pending_with_lock_key`
/// reads it for an O(1) lookup instead of scanning all pending bodies.
pub fn job_lock_key_index_path(queue: &str, lock_key: &str) -> String {
    format!(
        "{JOBS_ROOT}/index/{queue}/{}.json",
        encode_job_lock_key(lock_key)
    )
}

/// Percent-encode characters that are unsafe as a filesystem filename or as
/// part of an S3 key path component, so a `lock_key` lands on the same path
/// regardless of backend.
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

pub fn link_path(link: &LinkKind, namespace: &str) -> String {
    format!("{}/link", link_container_path(link, namespace))
}

pub fn link_container_path(link: &LinkKind, namespace: &str) -> String {
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
        assert_eq!(
            upload_container_path("ns", "uuid"),
            "v2/repositories/ns/_uploads/uuid"
        );
        assert_eq!(
            upload_path("ns", "uuid"),
            "v2/repositories/ns/_uploads/uuid/data"
        );
        assert_eq!(uploads_root_dir("ns"), "v2/repositories/ns/_uploads");
        assert_eq!(
            upload_hash_context_path("ns", "uuid", "sha256", 42),
            "v2/repositories/ns/_uploads/uuid/hashstates/sha256/42"
        );
        assert_eq!(
            upload_start_date_path("ns", "uuid"),
            "v2/repositories/ns/_uploads/uuid/startedat"
        );
    }

    #[test]
    fn test_manifest_paths() {
        assert_eq!(
            manifest_revisions_link_root_dir("ns", "sha256"),
            "v2/repositories/ns/_manifests/revisions/sha256"
        );
        assert_eq!(
            manifest_tags_dir("ns"),
            "v2/repositories/ns/_manifests/tags"
        );
        assert_eq!(
            manifest_tag_dir("ns", "v1.0"),
            "v2/repositories/ns/_manifests/tags/v1.0"
        );

        let subject = Digest::sha256(HASH_A).unwrap();
        assert_eq!(
            manifest_referrers_dir("ns", &subject),
            format!("v2/repositories/ns/_manifests/referrers/sha256/{HASH_A}")
        );
    }

    #[test]
    fn test_link_paths() {
        let digest = Digest::sha256(HASH_A).unwrap();

        let blob = LinkKind::Blob(digest.clone());
        assert_eq!(
            link_path(&blob, "ns"),
            format!("v2/repositories/ns/_blobs/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&blob, "ns"),
            format!("v2/repositories/ns/_blobs/sha256/{HASH_A}")
        );

        let tag = LinkKind::Tag(Tag::new("v1.0").unwrap());
        assert_eq!(
            link_path(&tag, "ns"),
            "v2/repositories/ns/_manifests/tags/v1.0/current/link"
        );
        assert_eq!(
            link_container_path(&tag, "ns"),
            "v2/repositories/ns/_manifests/tags/v1.0/current"
        );

        let revision = LinkKind::Digest(digest.clone());
        assert_eq!(
            link_path(&revision, "ns"),
            format!("v2/repositories/ns/_manifests/revisions/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&revision, "ns"),
            format!("v2/repositories/ns/_manifests/revisions/sha256/{HASH_A}")
        );

        let layer = LinkKind::Layer(digest.clone());
        assert_eq!(
            link_path(&layer, "ns"),
            format!("v2/repositories/ns/_layers/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&layer, "ns"),
            format!("v2/repositories/ns/_layers/sha256/{HASH_A}")
        );

        let config = LinkKind::Config(digest.clone());
        assert_eq!(
            link_path(&config, "ns"),
            format!("v2/repositories/ns/_config/sha256/{HASH_A}/link")
        );
        assert_eq!(
            link_container_path(&config, "ns"),
            format!("v2/repositories/ns/_config/sha256/{HASH_A}")
        );

        let subject = Digest::sha256(HASH_A).unwrap();
        let referrer = Digest::sha256(HASH_B).unwrap();
        let referrer_link = LinkKind::Referrer(subject, referrer);
        assert_eq!(
            link_path(&referrer_link, "ns"),
            format!("v2/repositories/ns/_manifests/referrers/sha256/{HASH_A}/sha256/{HASH_B}/link")
        );
        assert_eq!(
            link_container_path(&referrer_link, "ns"),
            format!("v2/repositories/ns/_manifests/referrers/sha256/{HASH_A}/sha256/{HASH_B}")
        );

        let index = Digest::sha256(HASH_A).unwrap();
        let child = Digest::sha256(HASH_B).unwrap();
        let manifest_link = LinkKind::Manifest(index, child);
        assert_eq!(
            link_path(&manifest_link, "ns"),
            format!("v2/repositories/ns/_manifests/index/sha256/{HASH_A}/sha256/{HASH_B}/link")
        );
        assert_eq!(
            link_container_path(&manifest_link, "ns"),
            format!("v2/repositories/ns/_manifests/index/sha256/{HASH_A}/sha256/{HASH_B}")
        );
    }

    #[test]
    fn test_job_paths() {
        assert_eq!(job_pending_dir("cache"), "_jobs/pending/cache");
        assert_eq!(
            job_pending_path("cache", "01HABCDE"),
            "_jobs/pending/cache/01HABCDE.json"
        );
        assert_eq!(
            job_failed_path("cache", "01HABCDE"),
            "_jobs/failed/cache/01HABCDE.json"
        );
        assert_eq!(
            job_lock_key_index_path("cache", "cache.ns:sha256:abc"),
            "_jobs/index/cache/cache.ns%3Asha256%3Aabc.json"
        );
    }
}
