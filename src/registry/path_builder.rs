use crate::{oci::Digest, registry::metadata_store::link_kind::LinkKind, util::sha256};

const BLOBS_ROOT: &str = "v2/blobs";
const REPOS_ROOT: &str = "v2/repositories";
const REGISTRY_ROOT: &str = "_registry";
const JOBS_ROOT: &str = "_jobs";

pub fn blobs_root_dir() -> &'static str {
    BLOBS_ROOT
}

pub fn repository_dir() -> &'static str {
    REPOS_ROOT
}

pub fn namespace_registry_path() -> String {
    format!("{REGISTRY_ROOT}/namespaces.json")
}

pub fn namespace_registry_shard_dir() -> String {
    format!("{REGISTRY_ROOT}/ns")
}

pub fn namespace_registry_shard_path(namespace: &str) -> String {
    let shard = namespace_shard_key(namespace);
    format!("{REGISTRY_ROOT}/ns/{shard}.json")
}

// SHA-256 is used here (not DefaultHasher) because DefaultHasher is explicitly not
// guaranteed to be stable across Rust versions. Changing the hash algorithm after
// deployment would remap existing data to different shard paths, corrupting storage.
pub fn shard_key(value: &str) -> String {
    sha256::shard_key(value)
}

pub fn namespace_shard_key(namespace: &str) -> String {
    shard_key(namespace)
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

pub fn upload_container_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}")
}

pub fn upload_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/data")
}

/// Storage key used by the S3 backend to stash the per-session sub-part
/// remainder between PATCH calls. Lives under the upload container so the
/// regular container cleanup catches it.
pub fn upload_patch_pending_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/patches/pending")
}

/// Path for a durable upload-session record written by the engine-backed
/// upload path. Lives under `upload-sessions/` — a top-level prefix that does
/// not collide with any canonical key family.
///
/// Format: `upload-sessions/<namespace>/<uuid>.json`
pub fn upload_session_path(namespace: &str, uuid: &str) -> String {
    format!("upload-sessions/{namespace}/{uuid}.json")
}

/// Prefix under which all upload sessions for a namespace are stored.
///
/// Used by `BlobStore::list_uploads` and the recovery sweeper to enumerate sessions.
pub fn upload_sessions_namespace_prefix(namespace: &str) -> String {
    format!("upload-sessions/{namespace}/")
}

pub fn manifest_revisions_link_root_dir(namespace: &str, algorithm: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/revisions/{algorithm}")
}

pub fn manifest_tags_dir(namespace: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_manifests/tags")
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

    #[test]
    fn test_blob_paths() {
        let digest = Digest::Sha256("1234567890abcdef".into());
        assert_eq!(
            blob_path(&digest),
            "v2/blobs/sha256/12/1234567890abcdef/data"
        );
        assert_eq!(
            blob_index_path(&digest),
            "v2/blobs/sha256/12/1234567890abcdef/index.json"
        );
        assert_eq!(
            blob_container_dir(&digest),
            "v2/blobs/sha256/12/1234567890abcdef"
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
        assert_eq!(
            upload_session_path("ns", "uuid"),
            "upload-sessions/ns/uuid.json"
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

        let subject = Digest::Sha256("subject123".into());
        assert_eq!(
            manifest_referrers_dir("ns", &subject),
            "v2/repositories/ns/_manifests/referrers/sha256/subject123"
        );
    }

    #[test]
    fn test_link_paths() {
        let digest = Digest::Sha256("digest123".into());

        let blob = LinkKind::Blob(digest.clone());
        assert_eq!(
            link_path(&blob, "ns"),
            "v2/repositories/ns/_blobs/sha256/digest123/link"
        );
        assert_eq!(
            link_container_path(&blob, "ns"),
            "v2/repositories/ns/_blobs/sha256/digest123"
        );

        let tag = LinkKind::Tag("v1.0".to_string());
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
            "v2/repositories/ns/_manifests/revisions/sha256/digest123/link"
        );
        assert_eq!(
            link_container_path(&revision, "ns"),
            "v2/repositories/ns/_manifests/revisions/sha256/digest123"
        );

        let layer = LinkKind::Layer(digest.clone());
        assert_eq!(
            link_path(&layer, "ns"),
            "v2/repositories/ns/_layers/sha256/digest123/link"
        );
        assert_eq!(
            link_container_path(&layer, "ns"),
            "v2/repositories/ns/_layers/sha256/digest123"
        );

        let config = LinkKind::Config(digest.clone());
        assert_eq!(
            link_path(&config, "ns"),
            "v2/repositories/ns/_config/sha256/digest123/link"
        );
        assert_eq!(
            link_container_path(&config, "ns"),
            "v2/repositories/ns/_config/sha256/digest123"
        );

        let subject = Digest::Sha256("subject456".into());
        let referrer = Digest::Sha256("referrer789".into());
        let referrer_link = LinkKind::Referrer(subject, referrer);
        assert_eq!(
            link_path(&referrer_link, "ns"),
            "v2/repositories/ns/_manifests/referrers/sha256/subject456/sha256/referrer789/link"
        );
        assert_eq!(
            link_container_path(&referrer_link, "ns"),
            "v2/repositories/ns/_manifests/referrers/sha256/subject456/sha256/referrer789"
        );

        let index = Digest::Sha256("index123".into());
        let child = Digest::Sha256("child456".into());
        let manifest_link = LinkKind::Manifest(index, child);
        assert_eq!(
            link_path(&manifest_link, "ns"),
            "v2/repositories/ns/_manifests/index/sha256/index123/sha256/child456/link"
        );
        assert_eq!(
            link_container_path(&manifest_link, "ns"),
            "v2/repositories/ns/_manifests/index/sha256/index123/sha256/child456"
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

    #[test]
    fn test_namespace_registry_shard_paths() {
        assert_eq!(namespace_registry_shard_dir(), "_registry/ns");

        let path = namespace_registry_shard_path("my-repo");
        assert!(path.starts_with("_registry/ns/"));
        assert!(
            std::path::Path::new(&path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        );

        // Same namespace always maps to the same shard
        assert_eq!(
            namespace_registry_shard_path("my-repo"),
            namespace_registry_shard_path("my-repo")
        );

        // Shard key is a 2-char hex string
        let key = namespace_shard_key("my-repo");
        assert_eq!(key.len(), 2);
        assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
