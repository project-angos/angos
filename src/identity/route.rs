use serde::Serialize;
use uuid::Uuid;

use crate::oci::{Digest, Namespace, Reference};

/// Route represents the parsed request path and action.
/// Serializes to a flat structure compatible with CEL policy expressions.
///
/// Available fields in CEL policies:
/// - `action`: The action being performed (always present)
/// - `namespace`: The repository namespace (when applicable)
/// - `digest`: The blob/manifest digest (when applicable)
/// - `reference`: The manifest tag or digest reference (when applicable)
/// - `uuid`: The upload session UUID (for upload operations)
/// - `n`: Maximum number of results for pagination
/// - `last`: Last result marker for pagination
/// - `artifact_type`: Filter for referrer queries
#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum Route<'a> {
    #[serde(rename = "ui-asset")]
    UiAsset {
        #[serde(skip)]
        path: &'a str,
    },
    #[serde(rename = "ui-config")]
    UiConfig,
    Healthz,
    Readyz,
    Metrics,
    #[serde(rename = "get-api-version")]
    ApiVersion,
    #[serde(rename = "list-catalog")]
    ListCatalog {
        #[serde(skip_serializing_if = "Option::is_none")]
        n: Option<u16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last: Option<String>,
    },
    #[serde(rename = "list-tags")]
    ListTags {
        namespace: Namespace,
        #[serde(skip_serializing_if = "Option::is_none")]
        n: Option<u16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last: Option<String>,
    },
    #[serde(rename = "start-upload")]
    StartUpload {
        namespace: Namespace,
        #[serde(skip_serializing_if = "Option::is_none")]
        digest: Option<Digest>,
    },
    #[serde(rename = "get-upload")]
    GetUpload {
        namespace: Namespace,
        uuid: Uuid,
    },
    #[serde(rename = "update-upload")]
    PatchUpload {
        namespace: Namespace,
        uuid: Uuid,
    },
    #[serde(rename = "complete-upload")]
    PutUpload {
        namespace: Namespace,
        digest: Digest,
        uuid: Uuid,
    },
    #[serde(rename = "cancel-upload")]
    DeleteUpload {
        namespace: Namespace,
        uuid: Uuid,
    },
    #[serde(rename = "get-blob")]
    GetBlob {
        namespace: Namespace,
        digest: Digest,
    },
    #[serde(rename = "get-blob")]
    HeadBlob {
        namespace: Namespace,
        digest: Digest,
    },
    #[serde(rename = "delete-blob")]
    DeleteBlob {
        namespace: Namespace,
        digest: Digest,
    },
    #[serde(rename = "get-manifest")]
    GetManifest {
        namespace: Namespace,
        reference: Reference,
    },
    #[serde(rename = "get-manifest")]
    HeadManifest {
        namespace: Namespace,
        reference: Reference,
    },
    #[serde(rename = "put-manifest")]
    PutManifest {
        namespace: Namespace,
        reference: Reference,
    },
    #[serde(rename = "delete-manifest")]
    DeleteManifest {
        namespace: Namespace,
        reference: Reference,
    },
    #[serde(rename = "get-referrers")]
    GetReferrer {
        namespace: Namespace,
        digest: Digest,
        #[serde(skip_serializing_if = "Option::is_none")]
        artifact_type: Option<String>,
    },
    #[serde(rename = "list-revisions")]
    ListRevisions {
        namespace: Namespace,
    },
    #[serde(rename = "list-uploads")]
    ListUploads {
        namespace: Namespace,
    },
    #[serde(rename = "list-repositories")]
    ListRepositories,
    #[serde(rename = "list-namespaces")]
    ListNamespaces {
        repository: &'a str,
    },
    #[serde(rename = "unknown")]
    Unknown,
}

impl Route<'_> {
    pub fn get_namespace(&self) -> Option<&Namespace> {
        match self {
            Route::ListTags { namespace, .. }
            | Route::StartUpload { namespace, .. }
            | Route::GetUpload { namespace, .. }
            | Route::PatchUpload { namespace, .. }
            | Route::PutUpload { namespace, .. }
            | Route::DeleteUpload { namespace, .. }
            | Route::GetBlob { namespace, .. }
            | Route::HeadBlob { namespace, .. }
            | Route::DeleteBlob { namespace, .. }
            | Route::GetManifest { namespace, .. }
            | Route::HeadManifest { namespace, .. }
            | Route::PutManifest { namespace, .. }
            | Route::DeleteManifest { namespace, .. }
            | Route::GetReferrer { namespace, .. }
            | Route::ListRevisions { namespace, .. }
            | Route::ListUploads { namespace, .. } => Some(namespace),
            _ => None,
        }
    }

    pub fn get_digest(&self) -> Option<&Digest> {
        match self {
            Route::GetBlob { digest, .. }
            | Route::HeadBlob { digest, .. }
            | Route::DeleteBlob { digest, .. }
            | Route::GetReferrer { digest, .. }
            | Route::PutUpload { digest, .. } => Some(digest),
            Route::StartUpload { digest, .. } => digest.as_ref(),
            _ => None,
        }
    }

    pub fn get_reference(&self) -> Option<&Reference> {
        match self {
            Route::GetManifest { reference, .. }
            | Route::HeadManifest { reference, .. }
            | Route::PutManifest { reference, .. }
            | Route::DeleteManifest { reference, .. } => Some(reference),
            _ => None,
        }
    }

    pub fn action_name(&self) -> &'static str {
        match self {
            Route::UiAsset { .. } => "ui-asset",
            Route::UiConfig => "ui-config",
            Route::ApiVersion => "get-api-version",
            Route::Healthz => "healthz",
            Route::Readyz => "readyz",
            Route::Metrics => "metrics",
            Route::ListCatalog { .. } => "list-catalog",
            Route::ListTags { .. } => "list-tags",
            Route::StartUpload { .. } => "start-upload",
            Route::GetUpload { .. } => "get-upload",
            Route::PatchUpload { .. } => "update-upload",
            Route::PutUpload { .. } => "complete-upload",
            Route::DeleteUpload { .. } => "cancel-upload",
            Route::GetBlob { .. } | Route::HeadBlob { .. } => "get-blob",
            Route::DeleteBlob { .. } => "delete-blob",
            Route::GetManifest { .. } | Route::HeadManifest { .. } => "get-manifest",
            Route::PutManifest { .. } => "put-manifest",
            Route::DeleteManifest { .. } => "delete-manifest",
            Route::GetReferrer { .. } => "get-referrers",
            Route::ListRevisions { .. } => "list-revisions",
            Route::ListUploads { .. } => "list-uploads",
            Route::ListRepositories => "list-repositories",
            Route::ListNamespaces { .. } => "list-namespaces",
            Route::Unknown => "unknown",
        }
    }

    pub fn is_push(&self) -> bool {
        matches!(
            self,
            Route::StartUpload { .. }
                | Route::PatchUpload { .. }
                | Route::PutUpload { .. }
                | Route::DeleteUpload { .. }
                | Route::PutManifest { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::oci::Digest;

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

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_is_push_all_variants() {
        let cases: &[(Route<'_>, bool)] = &[
            (Route::UiAsset { path: "/" }, false),
            (Route::UiConfig, false),
            (Route::Healthz, false),
            (Route::Readyz, false),
            (Route::Metrics, false),
            (Route::ApiVersion, false),
            (
                Route::ListCatalog {
                    n: None,
                    last: None,
                },
                false,
            ),
            (
                Route::ListTags {
                    namespace: ns(),
                    n: None,
                    last: None,
                },
                false,
            ),
            (
                Route::StartUpload {
                    namespace: ns(),
                    digest: None,
                },
                true,
            ),
            (
                Route::GetUpload {
                    namespace: ns(),
                    uuid: Uuid::nil(),
                },
                false,
            ),
            (
                Route::PatchUpload {
                    namespace: ns(),
                    uuid: Uuid::nil(),
                },
                true,
            ),
            (
                Route::PutUpload {
                    namespace: ns(),
                    digest: digest(),
                    uuid: Uuid::nil(),
                },
                true,
            ),
            (
                Route::DeleteUpload {
                    namespace: ns(),
                    uuid: Uuid::nil(),
                },
                true,
            ),
            (
                Route::GetBlob {
                    namespace: ns(),
                    digest: digest(),
                },
                false,
            ),
            (
                Route::HeadBlob {
                    namespace: ns(),
                    digest: digest(),
                },
                false,
            ),
            (
                Route::DeleteBlob {
                    namespace: ns(),
                    digest: digest(),
                },
                false,
            ),
            (
                Route::GetManifest {
                    namespace: ns(),
                    reference: reference(),
                },
                false,
            ),
            (
                Route::HeadManifest {
                    namespace: ns(),
                    reference: reference(),
                },
                false,
            ),
            (
                Route::PutManifest {
                    namespace: ns(),
                    reference: reference(),
                },
                true,
            ),
            (
                Route::DeleteManifest {
                    namespace: ns(),
                    reference: reference(),
                },
                false,
            ),
            (
                Route::GetReferrer {
                    namespace: ns(),
                    digest: digest(),
                    artifact_type: None,
                },
                false,
            ),
            (Route::ListRevisions { namespace: ns() }, false),
            (Route::ListUploads { namespace: ns() }, false),
            (Route::ListRepositories, false),
            (Route::ListNamespaces { repository: "test" }, false),
            (Route::Unknown, false),
        ];

        for (route, expected) in cases {
            // The exhaustive match below is the compile-time enforcement mechanism.
            // If a new variant is added without updating this test, it will not compile.
            let expected_from_match = match route {
                Route::StartUpload { .. }
                | Route::PatchUpload { .. }
                | Route::PutUpload { .. }
                | Route::DeleteUpload { .. }
                | Route::PutManifest { .. } => true,
                Route::UiAsset { .. }
                | Route::UiConfig
                | Route::Healthz
                | Route::Readyz
                | Route::Metrics
                | Route::ApiVersion
                | Route::ListCatalog { .. }
                | Route::ListTags { .. }
                | Route::GetUpload { .. }
                | Route::GetBlob { .. }
                | Route::HeadBlob { .. }
                | Route::DeleteBlob { .. }
                | Route::GetManifest { .. }
                | Route::HeadManifest { .. }
                | Route::DeleteManifest { .. }
                | Route::GetReferrer { .. }
                | Route::ListRevisions { .. }
                | Route::ListUploads { .. }
                | Route::ListRepositories
                | Route::ListNamespaces { .. }
                | Route::Unknown => false,
            };
            assert_eq!(
                expected_from_match, *expected,
                "expected_from_match disagrees with table for {route:?}"
            );
            assert_eq!(route.is_push(), *expected, "is_push() wrong for {route:?}");
        }
    }

    #[test]
    fn test_get_digest_with_digest() {
        let d = digest();

        let route = Route::GetBlob {
            namespace: ns(),
            digest: d.clone(),
        };
        assert_eq!(route.get_digest(), Some(&d));

        let route = Route::HeadBlob {
            namespace: ns(),
            digest: d.clone(),
        };
        assert_eq!(route.get_digest(), Some(&d));

        let route = Route::DeleteBlob {
            namespace: ns(),
            digest: d.clone(),
        };
        assert_eq!(route.get_digest(), Some(&d));

        let route = Route::GetReferrer {
            namespace: ns(),
            digest: d.clone(),
            artifact_type: None,
        };
        assert_eq!(route.get_digest(), Some(&d));

        let route = Route::PutUpload {
            namespace: ns(),
            digest: d.clone(),
            uuid: Uuid::nil(),
        };
        assert_eq!(route.get_digest(), Some(&d));

        let route = Route::StartUpload {
            namespace: ns(),
            digest: Some(d.clone()),
        };
        assert_eq!(route.get_digest(), Some(&d));
    }

    #[test]
    fn test_get_digest_without_digest() {
        let route = Route::StartUpload {
            namespace: ns(),
            digest: None,
        };
        assert_eq!(route.get_digest(), None);

        assert_eq!(Route::ApiVersion.get_digest(), None);
        assert_eq!(
            Route::ListCatalog {
                n: None,
                last: None
            }
            .get_digest(),
            None
        );
        assert_eq!(
            Route::ListTags {
                namespace: ns(),
                n: None,
                last: None
            }
            .get_digest(),
            None
        );
        assert_eq!(
            Route::GetManifest {
                namespace: ns(),
                reference: reference()
            }
            .get_digest(),
            None
        );
        assert_eq!(Route::Unknown.get_digest(), None);
    }

    #[test]
    fn test_get_reference_with_reference() {
        let r = reference();

        let route = Route::GetManifest {
            namespace: ns(),
            reference: r.clone(),
        };
        assert_eq!(
            route.get_reference().map(Reference::as_str),
            Some(r.as_str())
        );

        let route = Route::HeadManifest {
            namespace: ns(),
            reference: r.clone(),
        };
        assert_eq!(
            route.get_reference().map(Reference::as_str),
            Some(r.as_str())
        );

        let route = Route::PutManifest {
            namespace: ns(),
            reference: r.clone(),
        };
        assert_eq!(
            route.get_reference().map(Reference::as_str),
            Some(r.as_str())
        );

        let route = Route::DeleteManifest {
            namespace: ns(),
            reference: r.clone(),
        };
        assert_eq!(
            route.get_reference().map(Reference::as_str),
            Some(r.as_str())
        );
    }

    #[test]
    fn test_get_reference_without_reference() {
        assert!(Route::ApiVersion.get_reference().is_none());
        assert!(
            Route::ListCatalog {
                n: None,
                last: None
            }
            .get_reference()
            .is_none()
        );
        assert!(
            Route::GetBlob {
                namespace: ns(),
                digest: digest()
            }
            .get_reference()
            .is_none()
        );
        assert!(
            Route::StartUpload {
                namespace: ns(),
                digest: None
            }
            .get_reference()
            .is_none()
        );
        assert!(Route::Unknown.get_reference().is_none());
    }

    #[test]
    fn test_serialization_compatibility() {
        // Test that the serialized format is consistent with the old ClientRequest

        // Test get-api-version (only action field)
        let route = Route::ApiVersion;
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "get-api-version");
        assert_eq!(json.get("namespace"), None);
        assert_eq!(json.get("digest"), None);
        assert_eq!(json.get("reference"), None);

        // Test get-manifest with namespace and reference
        let reference = Reference::from_str("v1.0.0").unwrap();
        let route = Route::GetManifest {
            namespace: Namespace::new("library/nginx").unwrap(),
            reference,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "get-manifest");
        assert_eq!(json["namespace"], "library/nginx");
        assert_eq!(
            json["reference"],
            serde_json::to_value(Reference::from_str("v1.0.0").unwrap()).unwrap()
        );
        assert_eq!(json.get("digest"), None);

        // Test get-blob with namespace and digest
        let digest = Digest::from_str(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let route = Route::GetBlob {
            namespace: Namespace::new("library/nginx").unwrap(),
            digest,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "get-blob");
        assert_eq!(json["namespace"], "library/nginx");
        assert_eq!(
            json["digest"],
            serde_json::to_value(
                Digest::from_str(
                    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                )
                .unwrap()
            )
            .unwrap()
        );
        assert_eq!(json.get("reference"), None);

        // Test start-upload with namespace
        let route = Route::StartUpload {
            namespace: Namespace::new("library/nginx").unwrap(),
            digest: None,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "start-upload");
        assert_eq!(json["namespace"], "library/nginx");
        assert_eq!(json.get("digest"), None);

        // Test list-catalog (only action field)
        let route = Route::ListCatalog {
            n: None,
            last: None,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "list-catalog");
        assert_eq!(json.get("namespace"), None);

        // Test list-tags with namespace
        let route = Route::ListTags {
            namespace: Namespace::new("library/nginx").unwrap(),
            n: Some(10),
            last: Some("library/alpine".to_string()),
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "list-tags");
        assert_eq!(json["namespace"], "library/nginx");
        // n and last are now included when present
        assert_eq!(json["n"], 10);
        assert_eq!(json["last"], "library/alpine");
    }

    #[test]
    fn test_get_namespace() {
        let route = Route::GetManifest {
            namespace: Namespace::new("library/nginx").unwrap(),
            reference: Reference::from_str("v1.0.0").unwrap(),
        };
        assert_eq!(
            route.get_namespace().map(std::convert::AsRef::as_ref),
            Some("library/nginx")
        );

        assert_eq!(Route::ApiVersion.get_namespace(), None);
        assert_eq!(
            Route::ListCatalog {
                n: None,
                last: None
            }
            .get_namespace(),
            None
        );
    }

    #[test]
    fn test_cel_policy_compatibility() {
        // Test that serialization provides the fields CEL policies expect

        // Test action-only routes
        let route = Route::ApiVersion;
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "get-api-version");
        assert!(json.is_object());

        // Test routes with namespace
        let route = Route::ListTags {
            namespace: Namespace::new("test-repo").unwrap(),
            n: None,
            last: None,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "list-tags");
        assert_eq!(json["namespace"], "test-repo");

        // Test routes with namespace and digest
        let digest = Digest::from_str(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let route = Route::GetBlob {
            namespace: Namespace::new("test-repo").unwrap(),
            digest,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "get-blob");
        assert_eq!(json["namespace"], "test-repo");
        assert_eq!(
            json["digest"],
            serde_json::to_value(
                Digest::from_str(
                    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                )
                .unwrap()
            )
            .unwrap()
        );

        // Test routes with namespace and reference
        let reference = Reference::from_str("v1.0.0").unwrap();
        let route = Route::PutManifest {
            namespace: Namespace::new("test-repo").unwrap(),
            reference,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "put-manifest");
        assert_eq!(json["namespace"], "test-repo");
        assert_eq!(
            json["reference"],
            serde_json::to_value(Reference::from_str("v1.0.0").unwrap()).unwrap()
        );

        // Test routes with UUID
        let uuid = Uuid::nil();
        let route = Route::GetUpload {
            namespace: Namespace::new("test-repo").unwrap(),
            uuid,
        };
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["action"], "get-upload");
        assert_eq!(json["namespace"], "test-repo");
        assert_eq!(json["uuid"], serde_json::to_value(Uuid::nil()).unwrap());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_all_actions_have_correct_names() {
        // Verify all action names match the expected format from ClientRequest
        let test_cases = vec![
            (Route::ApiVersion, "get-api-version"),
            (Route::Healthz, "healthz"),
            (Route::Readyz, "readyz"),
            (Route::Metrics, "metrics"),
            (
                Route::ListCatalog {
                    n: None,
                    last: None,
                },
                "list-catalog",
            ),
            (
                Route::ListTags {
                    namespace: Namespace::new("test").unwrap(),
                    n: None,
                    last: None,
                },
                "list-tags",
            ),
            (
                Route::StartUpload {
                    namespace: Namespace::new("test").unwrap(),
                    digest: None,
                },
                "start-upload",
            ),
            (
                Route::GetUpload {
                    namespace: Namespace::new("test").unwrap(),
                    uuid: Uuid::nil(),
                },
                "get-upload",
            ),
            (
                Route::PatchUpload {
                    namespace: Namespace::new("test").unwrap(),
                    uuid: Uuid::nil(),
                },
                "update-upload",
            ),
            (
                Route::PutUpload {
                    namespace: Namespace::new("test").unwrap(),
                    uuid: Uuid::nil(),
                    digest: Digest::from_str(
                        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    )
                    .unwrap(),
                },
                "complete-upload",
            ),
            (
                Route::DeleteUpload {
                    namespace: Namespace::new("test").unwrap(),
                    uuid: Uuid::nil(),
                },
                "cancel-upload",
            ),
            (
                Route::GetBlob {
                    namespace: Namespace::new("test").unwrap(),
                    digest: Digest::from_str(
                        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    )
                    .unwrap(),
                },
                "get-blob",
            ),
            (
                Route::HeadBlob {
                    namespace: Namespace::new("test").unwrap(),
                    digest: Digest::from_str(
                        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    )
                    .unwrap(),
                },
                "get-blob",
            ),
            (
                Route::DeleteBlob {
                    namespace: Namespace::new("test").unwrap(),
                    digest: Digest::from_str(
                        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    )
                    .unwrap(),
                },
                "delete-blob",
            ),
            (
                Route::GetManifest {
                    namespace: Namespace::new("test").unwrap(),
                    reference: Reference::from_str("v1.0.0").unwrap(),
                },
                "get-manifest",
            ),
            (
                Route::HeadManifest {
                    namespace: Namespace::new("test").unwrap(),
                    reference: Reference::from_str("v1.0.0").unwrap(),
                },
                "get-manifest",
            ),
            (
                Route::PutManifest {
                    namespace: Namespace::new("test").unwrap(),
                    reference: Reference::from_str("v1.0.0").unwrap(),
                },
                "put-manifest",
            ),
            (
                Route::DeleteManifest {
                    namespace: Namespace::new("test").unwrap(),
                    reference: Reference::from_str("v1.0.0").unwrap(),
                },
                "delete-manifest",
            ),
            (
                Route::GetReferrer {
                    namespace: Namespace::new("test").unwrap(),
                    digest: Digest::from_str(
                        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    )
                    .unwrap(),
                    artifact_type: None,
                },
                "get-referrers",
            ),
            (Route::Unknown, "unknown"),
        ];

        for (route, expected_action) in test_cases {
            let json = serde_json::to_value(&route).unwrap();
            assert_eq!(
                json["action"], expected_action,
                "Action mismatch for {route:?}",
            );
        }
    }
}
