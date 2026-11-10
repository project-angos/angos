use std::str::FromStr;

use serde_json::json;

use angos_oci::{Digest, Namespace, Reference, Tag, UploadSessionId};

use super::*;
use crate::jobs::{JobState, Queue};

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

fn job_key() -> JobKey {
    JobKey::try_from("0000018f-key".to_string()).unwrap()
}

fn session() -> UploadSessionId {
    UploadSessionId::generate()
}

/// One of every variant, with its CEL action name.
#[rustfmt::skip]
fn every_action() -> Vec<(&'static str, Action)> {
    vec![
        ("ui-asset", Action::UiAsset { path: "/_ui/app.js".to_string() }),
        ("ui-config", Action::UiConfig),
        ("get-token", Action::Token),
        ("healthz", Action::Healthz),
        ("readyz", Action::Readyz),
        ("metrics", Action::Metrics),
        ("get-api-version", Action::ApiVersion),
        ("list-catalog", Action::ListCatalog { n: None, last: None }),
        ("list-tags", Action::ListTags { namespace: ns(), n: None, last: None }),
        ("start-upload", Action::StartUpload { namespace: ns(), digest: None, digest_algorithm: None }),
        ("mount-blob", Action::MountBlob { namespace: ns(), digest: digest(), from: None }),
        ("get-upload", Action::GetUpload { namespace: ns(), session_id: session() }),
        ("update-upload", Action::PatchUpload { namespace: ns(), session_id: session() }),
        ("complete-upload", Action::PutUpload { namespace: ns(), digest: digest(), session_id: session() }),
        ("cancel-upload", Action::DeleteUpload { namespace: ns(), session_id: session() }),
        ("get-blob", Action::GetBlob { namespace: ns(), digest: digest() }),
        ("get-blob", Action::HeadBlob { namespace: ns(), digest: digest() }),
        ("get-blob", Action::ListLayerEntries { namespace: ns(), digest: digest() }),
        ("get-blob", Action::GetLayerFile { namespace: ns(), digest: digest(), path: "etc/os-release".to_string(), download: false }),
        ("get-blob", Action::GetLayerFileDetails { namespace: ns(), digest: digest(), path: "usr/bin/env".to_string() }),
        ("delete-blob", Action::DeleteBlob { namespace: ns(), digest: digest() }),
        ("get-manifest", Action::GetManifest { namespace: ns(), reference: reference() }),
        ("get-manifest", Action::HeadManifest { namespace: ns(), reference: reference() }),
        ("put-manifest", Action::PutManifest { namespace: ns(), target: ManifestPutTarget::Tag(Tag::new("v1.0.0").unwrap()) }),
        ("delete-manifest", Action::DeleteManifest { namespace: ns(), reference: reference() }),
        ("get-referrers", Action::GetReferrer { namespace: ns(), digest: digest(), artifact_type: None, last: None }),
        ("list-revisions", Action::ListRevisions { namespace: ns() }),
        ("list-revisions", Action::ListPulls { namespace: ns(), reference: reference() }),
        ("list-uploads", Action::ListUploads { namespace: ns() }),
        ("list-repositories", Action::ListRepositories),
        ("list-namespaces", Action::ListNamespaces { repository: ns() }),
        ("list-jobs", Action::ListJobs { queue: Queue::Cache, n: None, after: None }),
        ("list-failed-jobs", Action::ListFailedJobs { queue: Queue::Cache, n: None, after: None }),
        ("retry-job", Action::RetryJob { queue: Queue::Cache, storage_key: job_key() }),
        ("delete-job", Action::DeleteJob { queue: Queue::Cache, state: JobState::Failed, storage_key: job_key() }),
    ]
}

/// The CEL action name is declared twice per variant, by `#[serde(rename)]`
/// and by `action_name()` (the `X-Registry-Action` header); they must agree.
#[test]
fn serialized_action_matches_action_name() {
    for (expected, action) in every_action() {
        assert_eq!(
            serde_json::to_value(&action).unwrap()["action"],
            expected,
            "{action:?}"
        );
        assert_eq!(action.action_name(), expected, "{action:?}");
    }
}

/// The serialized form is the `request` object CEL rules read.
#[test]
fn cel_payload_shape() {
    let cases = [
        (
            Action::ListCatalog {
                n: None,
                last: None,
            },
            json!({"action": "list-catalog"}),
        ),
        (
            Action::ListTags {
                namespace: ns(),
                n: Some(10),
                last: Some("library/alpine".to_string()),
            },
            json!({"action": "list-tags", "namespace": "test", "n": 10, "last": "library/alpine"}),
        ),
        (
            Action::GetBlob {
                namespace: ns(),
                digest: digest(),
            },
            json!({"action": "get-blob", "namespace": "test", "digest": SHA256_EMPTY}),
        ),
        (
            Action::ListNamespaces { repository: ns() },
            json!({"action": "list-namespaces", "repository": "test"}),
        ),
        (
            Action::HeadManifest {
                namespace: ns(),
                reference: reference(),
            },
            json!({"action": "get-manifest", "namespace": "test", "reference": "v1.0.0"}),
        ),
        (
            Action::PutManifest {
                namespace: ns(),
                target: ManifestPutTarget::Tag(Tag::new("latest").unwrap()),
            },
            json!({"action": "put-manifest", "namespace": "test", "tags": ["latest"]}),
        ),
        (
            Action::PutManifest {
                namespace: ns(),
                target: ManifestPutTarget::Digest {
                    digest: digest(),
                    tags: vec![Tag::new("v1").unwrap(), Tag::new("v2").unwrap()],
                },
            },
            json!({"action": "put-manifest", "namespace": "test", "digest": SHA256_EMPTY, "tags": ["v1", "v2"]}),
        ),
    ];

    for (action, expected) in cases {
        assert_eq!(serde_json::to_value(&action).unwrap(), expected);
    }
}

#[rustfmt::skip]
#[test]
fn accessors_follow_the_action_kind() {
    let d = digest();
    // (action, has namespace, digest, is_push)
    let cases = [
        (Action::ApiVersion, false, None, false),
        (Action::ListRepositories, false, None, false),
        (Action::RetryJob { queue: Queue::Cache, storage_key: job_key() }, false, None, true),
        (Action::GetManifest { namespace: ns(), reference: reference() }, true, None, false),
        (Action::DeleteManifest { namespace: ns(), reference: reference() }, true, None, false),
        (Action::PatchUpload { namespace: ns(), session_id: session() }, true, None, true),
        (Action::StartUpload { namespace: ns(), digest: None, digest_algorithm: None }, true, None, true),
        (Action::StartUpload { namespace: ns(), digest: Some(d.clone()), digest_algorithm: None }, true, Some(&d), true),
        (Action::MountBlob { namespace: ns(), digest: d.clone(), from: Some(ns()) }, true, Some(&d), true),
        (Action::PutUpload { namespace: ns(), digest: d.clone(), session_id: session() }, true, Some(&d), true),
        (Action::GetBlob { namespace: ns(), digest: d.clone() }, true, Some(&d), false),
        (Action::DeleteBlob { namespace: ns(), digest: d.clone() }, true, Some(&d), false),
        (Action::PutManifest { namespace: ns(), target: ManifestPutTarget::Tag(Tag::new("v1").unwrap()) }, true, None, true),
    ];

    for (action, has_namespace, digest, is_push) in &cases {
        assert_eq!(action.namespace().is_some(), *has_namespace, "{action:?}");
        assert_eq!(action.digest(), *digest, "{action:?}");
        assert_eq!(action.is_push(), *is_push, "{action:?}");
    }

    let get = Action::GetManifest { namespace: ns(), reference: reference() };
    assert_eq!(get.reference().map(|r| r.to_string()).as_deref(), Some("v1.0.0"));
    assert!(Action::GetBlob { namespace: ns(), digest: d }.reference().is_none());
}
