use crate::{oci::Digest, registry::metadata_store::link_kind::LinkKind};

const BLOBS_ROOT: &str = "v2/blobs";
const REPOS_ROOT: &str = "v2/repositories";

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

pub fn blob_container_dir(digest: &Digest) -> String {
    blob_dir(digest)
}

pub fn uploads_root_dir(namespace: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads")
}

pub fn upload_container_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}")
}

pub fn upload_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/data")
}

pub fn upload_staged_container_path(namespace: &str, uuid: &str, offset: u64) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/staged/{offset}")
}

pub fn upload_hash_context_path(
    namespace: &str,
    uuid: &str,
    algorithm: &str,
    offset: u64,
) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/hashstates/{algorithm}/{offset}")
}

pub fn upload_start_date_path(namespace: &str, uuid: &str) -> String {
    format!("{REPOS_ROOT}/{namespace}/_uploads/{uuid}/startedat")
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

pub fn link_path(link: &LinkKind, namespace: &str) -> String {
    format!("{}/link", link_container_path(link, namespace))
}

pub fn link_container_path(link: &LinkKind, namespace: &str) -> String {
    match link {
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
        assert_eq!(uploads_root_dir("ns"), "v2/repositories/ns/_uploads");
        assert_eq!(
            upload_container_path("ns", "uuid"),
            "v2/repositories/ns/_uploads/uuid"
        );
        assert_eq!(
            upload_path("ns", "uuid"),
            "v2/repositories/ns/_uploads/uuid/data"
        );
        assert_eq!(
            upload_staged_container_path("ns", "uuid", 0),
            "v2/repositories/ns/_uploads/uuid/staged/0"
        );
        assert_eq!(
            upload_hash_context_path("ns", "uuid", "sha256", 0),
            "v2/repositories/ns/_uploads/uuid/hashstates/sha256/0"
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

        let subject = Digest::Sha256("subject123".into());
        assert_eq!(
            manifest_referrers_dir("ns", &subject),
            "v2/repositories/ns/_manifests/referrers/sha256/subject123"
        );
    }

    #[test]
    fn test_link_paths() {
        let digest = Digest::Sha256("digest123".into());

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
}
