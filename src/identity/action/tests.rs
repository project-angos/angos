use std::str::FromStr;

use super::*;
use crate::oci::{Digest, Namespace, Reference};

const SHA256_EMPTY: &str =
    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn ns() -> Namespace {
    Namespace::new("test").unwrap()
}

fn digest() -> Digest {
    Digest::from_str(SHA256_EMPTY).unwrap()
}

fn reference() -> Reference {
    Reference::from_str("v1.0.0").unwrap()
}

/// Verify every `Action` variant serializes to the exact kebab-case action string
/// that existing CEL policies depend on.
#[test]
#[allow(clippy::too_many_lines)]
fn test_action_serialization_cel_compatibility() {
    let cases: &[(&str, Action)] = &[
        (
            "ui-asset",
            Action::UiAsset {
                path: "/_ui/app.js".to_string(),
            },
        ),
        ("ui-config", Action::UiConfig),
        ("healthz", Action::Healthz),
        ("readyz", Action::Readyz),
        ("metrics", Action::Metrics),
        ("get-api-version", Action::ApiVersion),
        (
            "list-catalog",
            Action::ListCatalog {
                n: None,
                last: None,
            },
        ),
        (
            "list-tags",
            Action::ListTags {
                namespace: ns(),
                n: None,
                last: None,
            },
        ),
        (
            "start-upload",
            Action::StartUpload {
                namespace: ns(),
                digest: None,
            },
        ),
        (
            "get-upload",
            Action::GetUpload {
                namespace: ns(),
                uuid: Uuid::nil(),
            },
        ),
        (
            "update-upload",
            Action::PatchUpload {
                namespace: ns(),
                uuid: Uuid::nil(),
            },
        ),
        (
            "complete-upload",
            Action::PutUpload {
                namespace: ns(),
                digest: digest(),
                uuid: Uuid::nil(),
            },
        ),
        (
            "cancel-upload",
            Action::DeleteUpload {
                namespace: ns(),
                uuid: Uuid::nil(),
            },
        ),
        (
            "get-blob",
            Action::GetBlob {
                namespace: ns(),
                digest: digest(),
            },
        ),
        (
            "delete-blob",
            Action::DeleteBlob {
                namespace: ns(),
                digest: digest(),
            },
        ),
        (
            "get-manifest",
            Action::GetManifest {
                namespace: ns(),
                reference: reference(),
            },
        ),
        (
            "put-manifest",
            Action::PutManifest {
                namespace: ns(),
                reference: reference(),
            },
        ),
        (
            "delete-manifest",
            Action::DeleteManifest {
                namespace: ns(),
                reference: reference(),
            },
        ),
        (
            "get-referrers",
            Action::GetReferrer {
                namespace: ns(),
                digest: digest(),
                artifact_type: None,
            },
        ),
        ("list-revisions", Action::ListRevisions { namespace: ns() }),
        ("list-uploads", Action::ListUploads { namespace: ns() }),
        ("list-repositories", Action::ListRepositories),
        (
            "list-namespaces",
            Action::ListNamespaces {
                repository: Namespace::new("test").unwrap(),
            },
        ),
    ];

    for (expected_action_str, action) in cases {
        let json = serde_json::to_value(action).unwrap();
        assert_eq!(
            json["action"], *expected_action_str,
            "CEL action string mismatch for {action:?}"
        );
        assert_eq!(
            action.action_name(),
            *expected_action_str,
            "action_name() mismatch for {action:?}"
        );
    }
}

#[test]
fn test_list_namespaces_repository_serializes_as_plain_string() {
    let action = Action::ListNamespaces {
        repository: Namespace::new("myrepo").unwrap(),
    };
    let json = serde_json::to_value(&action).unwrap();
    assert_eq!(json["action"], "list-namespaces");
    assert_eq!(json["repository"], "myrepo");
}

#[test]
fn test_get_blob_includes_namespace_and_digest() {
    let action = Action::GetBlob {
        namespace: Namespace::new("library/nginx").unwrap(),
        digest: digest(),
    };
    let json = serde_json::to_value(&action).unwrap();
    assert_eq!(json["action"], "get-blob");
    assert_eq!(json["namespace"], "library/nginx");
    assert!(json.get("digest").is_some());
    assert!(json.get("reference").is_none());
}

#[test]
fn test_get_manifest_includes_namespace_and_reference() {
    let action = Action::GetManifest {
        namespace: Namespace::new("library/nginx").unwrap(),
        reference: Reference::from_str("v1.0.0").unwrap(),
    };
    let json = serde_json::to_value(&action).unwrap();
    assert_eq!(json["action"], "get-manifest");
    assert_eq!(json["namespace"], "library/nginx");
    assert!(json.get("reference").is_some());
    assert!(json.get("digest").is_none());
}

#[test]
fn test_list_catalog_omits_null_pagination() {
    let action = Action::ListCatalog {
        n: None,
        last: None,
    };
    let json = serde_json::to_value(&action).unwrap();
    assert_eq!(json["action"], "list-catalog");
    assert!(json.get("n").is_none());
    assert!(json.get("last").is_none());
}

#[test]
fn test_list_tags_with_pagination() {
    let action = Action::ListTags {
        namespace: Namespace::new("library/nginx").unwrap(),
        n: Some(10),
        last: Some("library/alpine".to_string()),
    };
    let json = serde_json::to_value(&action).unwrap();
    assert_eq!(json["action"], "list-tags");
    assert_eq!(json["n"], 10);
    assert_eq!(json["last"], "library/alpine");
}

#[test]
fn test_get_namespace() {
    assert_eq!(
        Action::GetBlob {
            namespace: ns(),
            digest: digest()
        }
        .get_namespace()
        .map(AsRef::as_ref),
        Some("test")
    );
    assert_eq!(Action::ApiVersion.get_namespace(), None);
    assert_eq!(
        Action::ListCatalog {
            n: None,
            last: None
        }
        .get_namespace(),
        None
    );
    assert_eq!(Action::ListRepositories.get_namespace(), None);
}

#[test]
fn test_get_digest() {
    let d = digest();
    assert_eq!(
        Action::GetBlob {
            namespace: ns(),
            digest: d.clone()
        }
        .get_digest(),
        Some(&d)
    );
    assert_eq!(
        Action::StartUpload {
            namespace: ns(),
            digest: Some(d.clone())
        }
        .get_digest(),
        Some(&d)
    );
    assert_eq!(
        Action::StartUpload {
            namespace: ns(),
            digest: None
        }
        .get_digest(),
        None
    );
    assert_eq!(Action::ApiVersion.get_digest(), None);
}

#[test]
fn test_get_reference() {
    let r = reference();
    assert_eq!(
        Action::GetManifest {
            namespace: ns(),
            reference: r.clone()
        }
        .get_reference()
        .map(Reference::to_string),
        Some(r.to_string())
    );
    assert!(Action::ApiVersion.get_reference().is_none());
    assert!(
        Action::GetBlob {
            namespace: ns(),
            digest: digest()
        }
        .get_reference()
        .is_none()
    );
}

#[test]
fn test_is_push() {
    assert!(
        Action::StartUpload {
            namespace: ns(),
            digest: None
        }
        .is_push()
    );
    assert!(
        Action::PatchUpload {
            namespace: ns(),
            uuid: uuid::Uuid::nil()
        }
        .is_push()
    );
    assert!(
        Action::PutUpload {
            namespace: ns(),
            digest: digest(),
            uuid: uuid::Uuid::nil()
        }
        .is_push()
    );
    assert!(
        Action::DeleteUpload {
            namespace: ns(),
            uuid: uuid::Uuid::nil()
        }
        .is_push()
    );
    assert!(
        Action::PutManifest {
            namespace: ns(),
            reference: reference()
        }
        .is_push()
    );

    assert!(!Action::ApiVersion.is_push());
    assert!(
        !Action::GetBlob {
            namespace: ns(),
            digest: digest()
        }
        .is_push()
    );
    assert!(
        !Action::GetManifest {
            namespace: ns(),
            reference: reference()
        }
        .is_push()
    );
    assert!(
        !Action::DeleteBlob {
            namespace: ns(),
            digest: digest()
        }
        .is_push()
    );
    assert!(
        !Action::DeleteManifest {
            namespace: ns(),
            reference: reference()
        }
        .is_push()
    );
}
