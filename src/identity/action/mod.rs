use serde::Serialize;
use uuid::Uuid;

use crate::{
    oci::{Digest, Namespace, Reference},
    registry::job_store::JobState,
};

/// Action represents a parsed HTTP request: both the domain operation (for CEL policies)
/// and the routing information (for handler dispatch).
///
/// HEAD and GET variants of blob/manifest requests are distinct variants so the dispatcher
/// can route them to different handlers, but they serialize to the same kebab-case string
/// so CEL policies cannot distinguish them:
///
/// ```text
/// Action::GetBlob  → "get-blob"    (CEL string)
/// Action::HeadBlob → "get-blob"    (same CEL string, different handler)
/// ```
///
/// # Serialization
///
/// Serializes to a flat JSON object compatible with CEL policy expressions:
///
/// ```json
/// {"action": "get-blob", "namespace": "library/nginx", "digest": "sha256:..."}
/// ```
///
/// HTTP-only fields such as `UiAsset.path` are excluded from the serialized form because
/// they have no meaning to the policy engine.
///
/// # Available Fields in CEL Policies
///
/// - `action`: The operation name (always present)
/// - `namespace`: The repository namespace (when applicable)
/// - `digest`: The blob/manifest digest (when applicable)
/// - `reference`: The manifest tag or digest reference (when applicable)
/// - `uuid`: The upload session UUID (for upload operations)
/// - `n`: Maximum number of results for pagination
/// - `last`: Last result marker for pagination
/// - `artifact_type`: Filter for referrer queries
/// - `from`: Source repository of a cross-repo `mount-blob` (present only when the mount specifies `?from=`; test with `has(request.from)`)
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum Action {
    #[serde(rename = "ui-asset")]
    UiAsset {
        #[serde(skip)]
        path: String,
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
    /// Cross-repository blob mount (`POST .../blobs/uploads/?mount=<digest>`). A
    /// distinct route and CEL action from `start-upload`, so a policy gates
    /// mounts with a plain `request.action == 'mount-blob'` rule. `digest` is the
    /// blob to mount; `from` is the optional source repository.
    #[serde(rename = "mount-blob")]
    MountBlob {
        namespace: Namespace,
        digest: Digest,
        #[serde(skip_serializing_if = "Option::is_none")]
        from: Option<Namespace>,
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
    /// Same CEL action name as `GetBlob`; distinct variant so the dispatcher can
    /// route HEAD requests to a body-less handler.
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
    /// Same CEL action name as `GetManifest`; distinct variant so the dispatcher
    /// can route HEAD requests to a body-less handler.
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
        repository: Namespace,
    },
    /// List pending/in-flight durable jobs. Distinct action name so operators
    /// can gate job administration behind higher privilege than registry reads.
    /// `queue` (the `cache` or `replication` queue) is exposed to CEL so a
    /// policy can gate replication-queue administration separately.
    #[serde(rename = "list-jobs")]
    ListJobs {
        queue: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        n: Option<u16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<String>,
    },
    #[serde(rename = "list-failed-jobs")]
    ListFailedJobs {
        queue: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        n: Option<u16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<String>,
    },
    /// Requeue a dead-letter job. `queue` is exposed to CEL (as for `list-jobs`);
    /// `storage_key` is HTTP routing only — excluded from the CEL payload like
    /// other addressing fields.
    #[serde(rename = "retry-job")]
    RetryJob {
        queue: String,
        #[serde(skip)]
        storage_key: String,
    },
    #[serde(rename = "delete-job")]
    DeleteJob {
        queue: String,
        #[serde(skip)]
        state: JobState,
        #[serde(skip)]
        storage_key: String,
    },
}

struct ActionData<'a> {
    namespace: Option<&'a Namespace>,
    digest: Option<&'a Digest>,
    reference: Option<&'a Reference>,
    is_push: bool,
}

impl ActionData<'_> {
    const fn none() -> Self {
        Self {
            namespace: None,
            digest: None,
            reference: None,
            is_push: false,
        }
    }
}

impl Action {
    /// Returns the action name string as used in CEL policies and webhook headers.
    pub fn action_name(&self) -> &'static str {
        match self {
            Action::UiAsset { .. } => "ui-asset",
            Action::UiConfig => "ui-config",
            Action::Healthz => "healthz",
            Action::Readyz => "readyz",
            Action::Metrics => "metrics",
            Action::ApiVersion => "get-api-version",
            Action::ListCatalog { .. } => "list-catalog",
            Action::ListTags { .. } => "list-tags",
            Action::StartUpload { .. } => "start-upload",
            Action::MountBlob { .. } => "mount-blob",
            Action::GetUpload { .. } => "get-upload",
            Action::PatchUpload { .. } => "update-upload",
            Action::PutUpload { .. } => "complete-upload",
            Action::DeleteUpload { .. } => "cancel-upload",
            Action::GetBlob { .. } | Action::HeadBlob { .. } => "get-blob",
            Action::DeleteBlob { .. } => "delete-blob",
            Action::GetManifest { .. } | Action::HeadManifest { .. } => "get-manifest",
            Action::PutManifest { .. } => "put-manifest",
            Action::DeleteManifest { .. } => "delete-manifest",
            Action::GetReferrer { .. } => "get-referrers",
            Action::ListRevisions { .. } => "list-revisions",
            Action::ListUploads { .. } => "list-uploads",
            Action::ListRepositories => "list-repositories",
            Action::ListNamespaces { .. } => "list-namespaces",
            Action::ListJobs { .. } => "list-jobs",
            Action::ListFailedJobs { .. } => "list-failed-jobs",
            Action::RetryJob { .. } => "retry-job",
            Action::DeleteJob { .. } => "delete-job",
        }
    }

    #[allow(clippy::too_many_lines)]
    fn action_data(&self) -> ActionData<'_> {
        match self {
            Action::UiAsset { .. }
            | Action::UiConfig
            | Action::Healthz
            | Action::Readyz
            | Action::Metrics
            | Action::ApiVersion
            | Action::ListCatalog { .. }
            | Action::ListRepositories
            | Action::ListNamespaces { .. }
            | Action::ListJobs { .. }
            | Action::ListFailedJobs { .. } => ActionData::none(),

            // Job mutations carry no namespace/digest, but flag `is_push` so they
            // are treated as state-changing for any write-sensitive policy logic.
            Action::RetryJob { .. } | Action::DeleteJob { .. } => ActionData {
                is_push: true,
                ..ActionData::none()
            },

            Action::ListTags { namespace, .. }
            | Action::GetUpload { namespace, .. }
            | Action::ListRevisions { namespace }
            | Action::ListUploads { namespace } => ActionData {
                namespace: Some(namespace),
                ..ActionData::none()
            },

            Action::PatchUpload { namespace, .. } | Action::DeleteUpload { namespace, .. } => {
                ActionData {
                    namespace: Some(namespace),
                    is_push: true,
                    ..ActionData::none()
                }
            }

            Action::StartUpload { namespace, digest } => ActionData {
                namespace: Some(namespace),
                digest: digest.as_ref(),
                is_push: true,
                ..ActionData::none()
            },

            Action::PutUpload {
                namespace, digest, ..
            }
            | Action::MountBlob {
                namespace, digest, ..
            } => ActionData {
                namespace: Some(namespace),
                digest: Some(digest),
                is_push: true,
                ..ActionData::none()
            },

            Action::GetBlob { namespace, digest }
            | Action::HeadBlob { namespace, digest }
            | Action::DeleteBlob { namespace, digest }
            | Action::GetReferrer {
                namespace, digest, ..
            } => ActionData {
                namespace: Some(namespace),
                digest: Some(digest),
                ..ActionData::none()
            },

            Action::GetManifest {
                namespace,
                reference,
            }
            | Action::HeadManifest {
                namespace,
                reference,
            }
            | Action::DeleteManifest {
                namespace,
                reference,
            } => ActionData {
                namespace: Some(namespace),
                reference: Some(reference),
                ..ActionData::none()
            },

            Action::PutManifest {
                namespace,
                reference,
            } => ActionData {
                namespace: Some(namespace),
                reference: Some(reference),
                is_push: true,
                ..ActionData::none()
            },
        }
    }

    /// Returns the namespace associated with this action, if any.
    pub fn get_namespace(&self) -> Option<&Namespace> {
        self.action_data().namespace
    }

    /// Returns the digest associated with this action, if any.
    pub fn get_digest(&self) -> Option<&Digest> {
        self.action_data().digest
    }

    /// Returns the manifest reference associated with this action, if any.
    pub fn get_reference(&self) -> Option<&Reference> {
        self.action_data().reference
    }

    /// Returns `true` if this action writes registry state (uploads or manifest puts).
    ///
    /// Used to reject push operations against pull-through cache repositories.
    pub fn is_push(&self) -> bool {
        self.action_data().is_push
    }
}

#[cfg(test)]
mod tests;
