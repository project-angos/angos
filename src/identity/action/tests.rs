use std::str::FromStr;

use super::*;
use crate::jobs::{JobState, Queue};
use crate::oci::{Digest, Namespace, Reference, Tag, UploadSessionId};

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

/// Drift guard: the CEL action name is declared twice per variant (the
/// `#[serde(rename)]` that drives the serialized `action` field, and the
/// `action_name()` arm that drives the `X-Registry-Action` header). This asserts
/// they agree for every variant; `assert_action_variant_covered` fails to compile
/// if a new variant is added without a case here.
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
            "mount-blob",
            Action::MountBlob {
                namespace: ns(),
                digest: digest(),
                from: None,
            },
        ),
        (
            "get-upload",
            Action::GetUpload {
                namespace: ns(),
                uuid: UploadSessionId::generate(),
            },
        ),
        (
            "update-upload",
            Action::PatchUpload {
                namespace: ns(),
                uuid: UploadSessionId::generate(),
            },
        ),
        (
            "complete-upload",
            Action::PutUpload {
                namespace: ns(),
                digest: digest(),
                uuid: UploadSessionId::generate(),
            },
        ),
        (
            "cancel-upload",
            Action::DeleteUpload {
                namespace: ns(),
                uuid: UploadSessionId::generate(),
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
                target: ManifestPutTarget::Tag(Tag::new("v1.0.0").unwrap()),
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
        (
            "get-blob",
            Action::HeadBlob {
                namespace: ns(),
                digest: digest(),
            },
        ),
        (
            "get-manifest",
            Action::HeadManifest {
                namespace: ns(),
                reference: reference(),
            },
        ),
        (
            "list-jobs",
            Action::ListJobs {
                queue: Queue::Cache,
                n: None,
                after: None,
            },
        ),
        (
            "list-failed-jobs",
            Action::ListFailedJobs {
                queue: Queue::Cache,
                n: None,
                after: None,
            },
        ),
        (
            "retry-job",
            Action::RetryJob {
                queue: Queue::Cache,
                storage_key: "0000018f-key".to_string(),
            },
        ),
        (
            "delete-job",
            Action::DeleteJob {
                queue: Queue::Cache,
                state: JobState::Failed,
                storage_key: "0000018f-key".to_string(),
            },
        ),
    ];

    for (expected_action_str, action) in cases {
        assert_action_variant_covered(action);
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

/// Compile-time exhaustiveness guard for the drift check above: a new `Action`
/// variant fails to compile here until it is added, a reminder to also add its
/// case to `cases`.
fn assert_action_variant_covered(action: &Action) {
    match action {
        Action::UiAsset { .. }
        | Action::UiConfig
        | Action::Healthz
        | Action::Readyz
        | Action::Metrics
        | Action::ApiVersion
        | Action::ListCatalog { .. }
        | Action::ListTags { .. }
        | Action::StartUpload { .. }
        | Action::MountBlob { .. }
        | Action::GetUpload { .. }
        | Action::PatchUpload { .. }
        | Action::PutUpload { .. }
        | Action::DeleteUpload { .. }
        | Action::GetBlob { .. }
        | Action::HeadBlob { .. }
        | Action::DeleteBlob { .. }
        | Action::GetManifest { .. }
        | Action::HeadManifest { .. }
        | Action::PutManifest { .. }
        | Action::DeleteManifest { .. }
        | Action::GetReferrer { .. }
        | Action::ListRevisions { .. }
        | Action::ListUploads { .. }
        | Action::ListRepositories
        | Action::ListNamespaces { .. }
        | Action::ListJobs { .. }
        | Action::ListFailedJobs { .. }
        | Action::RetryJob { .. }
        | Action::DeleteJob { .. } => {}
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
            digest: Some(d.clone()),
        }
        .get_digest(),
        Some(&d)
    );
    assert_eq!(
        Action::StartUpload {
            namespace: ns(),
            digest: None,
        }
        .get_digest(),
        None
    );
    // A mount-blob action exposes its mount-source digest as the action digest.
    assert_eq!(
        Action::MountBlob {
            namespace: ns(),
            digest: d.clone(),
            from: Some(ns()),
        }
        .get_digest(),
        Some(&d)
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
        .map(|r| r.to_string()),
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

// The read/delete manifest actions address a single reference, so they expose
// `reference` to CEL policies. A put addresses a digest and/or tags instead
// (see `test_put_manifest_target_serializes_digest_and_tags`).
#[test]
fn test_manifest_actions_serialize_reference_for_cel() {
    let r = reference();
    let actions = [
        Action::GetManifest {
            namespace: ns(),
            reference: r.clone(),
        },
        Action::HeadManifest {
            namespace: ns(),
            reference: r.clone(),
        },
        Action::DeleteManifest {
            namespace: ns(),
            reference: r.clone(),
        },
    ];
    for action in actions {
        let value = serde_json::to_value(&action).unwrap();
        assert!(
            value.get("reference").is_some(),
            "action {value} must expose `reference` to CEL"
        );
    }
}

// A put-manifest exposes `digest` (by-digest push) and `tags` (every tag the
// push creates) to CEL, each omitted when it does not apply.
#[test]
fn test_put_manifest_target_serializes_digest_and_tags() {
    let by_tag = Action::PutManifest {
        namespace: ns(),
        target: ManifestPutTarget::Tag(Tag::new("latest").unwrap()),
    };
    assert_eq!(
        serde_json::to_value(&by_tag).unwrap(),
        serde_json::json!({
            "action": "put-manifest",
            "namespace": "test",
            "tags": ["latest"],
        })
    );

    let by_digest = Action::PutManifest {
        namespace: ns(),
        target: ManifestPutTarget::Digest {
            digest: digest(),
            tags: vec![Tag::new("v1").unwrap(), Tag::new("v2").unwrap()],
        },
    };
    assert_eq!(
        serde_json::to_value(&by_digest).unwrap(),
        serde_json::json!({
            "action": "put-manifest",
            "namespace": "test",
            "digest": SHA256_EMPTY,
            "tags": ["v1", "v2"],
        })
    );

    let by_digest_no_tags = Action::PutManifest {
        namespace: ns(),
        target: ManifestPutTarget::Digest {
            digest: digest(),
            tags: Vec::new(),
        },
    };
    assert_eq!(
        serde_json::to_value(&by_digest_no_tags).unwrap(),
        serde_json::json!({
            "action": "put-manifest",
            "namespace": "test",
            "digest": SHA256_EMPTY,
            "tags": [],
        })
    );
}

#[test]
fn test_is_push() {
    assert!(
        Action::StartUpload {
            namespace: ns(),
            digest: None,
        }
        .is_push()
    );
    assert!(
        Action::MountBlob {
            namespace: ns(),
            digest: digest(),
            from: None,
        }
        .is_push()
    );
    assert!(
        Action::PatchUpload {
            namespace: ns(),
            uuid: UploadSessionId::generate()
        }
        .is_push()
    );
    assert!(
        Action::PutUpload {
            namespace: ns(),
            digest: digest(),
            uuid: UploadSessionId::generate()
        }
        .is_push()
    );
    assert!(
        Action::DeleteUpload {
            namespace: ns(),
            uuid: UploadSessionId::generate()
        }
        .is_push()
    );
    assert!(
        Action::PutManifest {
            namespace: ns(),
            target: ManifestPutTarget::Tag(Tag::new("v1.0.0").unwrap())
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
