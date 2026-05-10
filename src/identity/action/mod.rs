use serde::Serialize;
use uuid::Uuid;

use crate::oci::{Digest, Namespace, Reference};

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
        repository: String,
    },
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
        }
    }

    /// Returns the namespace associated with this action, if any.
    pub fn get_namespace(&self) -> Option<&Namespace> {
        match self {
            Action::ListTags { namespace, .. }
            | Action::StartUpload { namespace, .. }
            | Action::GetUpload { namespace, .. }
            | Action::PatchUpload { namespace, .. }
            | Action::PutUpload { namespace, .. }
            | Action::DeleteUpload { namespace, .. }
            | Action::GetBlob { namespace, .. }
            | Action::HeadBlob { namespace, .. }
            | Action::DeleteBlob { namespace, .. }
            | Action::GetManifest { namespace, .. }
            | Action::HeadManifest { namespace, .. }
            | Action::PutManifest { namespace, .. }
            | Action::DeleteManifest { namespace, .. }
            | Action::GetReferrer { namespace, .. }
            | Action::ListRevisions { namespace, .. }
            | Action::ListUploads { namespace, .. } => Some(namespace),
            Action::UiAsset { .. }
            | Action::UiConfig
            | Action::Healthz
            | Action::Readyz
            | Action::Metrics
            | Action::ApiVersion
            | Action::ListCatalog { .. }
            | Action::ListRepositories
            | Action::ListNamespaces { .. } => None,
        }
    }

    /// Returns the digest associated with this action, if any.
    pub fn get_digest(&self) -> Option<&Digest> {
        match self {
            Action::GetBlob { digest, .. }
            | Action::HeadBlob { digest, .. }
            | Action::DeleteBlob { digest, .. }
            | Action::GetReferrer { digest, .. }
            | Action::PutUpload { digest, .. } => Some(digest),
            Action::StartUpload { digest, .. } => digest.as_ref(),
            Action::UiAsset { .. }
            | Action::UiConfig
            | Action::Healthz
            | Action::Readyz
            | Action::Metrics
            | Action::ApiVersion
            | Action::ListCatalog { .. }
            | Action::ListTags { .. }
            | Action::GetUpload { .. }
            | Action::PatchUpload { .. }
            | Action::DeleteUpload { .. }
            | Action::GetManifest { .. }
            | Action::HeadManifest { .. }
            | Action::PutManifest { .. }
            | Action::DeleteManifest { .. }
            | Action::ListRevisions { .. }
            | Action::ListUploads { .. }
            | Action::ListRepositories
            | Action::ListNamespaces { .. } => None,
        }
    }

    /// Returns the manifest reference associated with this action, if any.
    pub fn get_reference(&self) -> Option<&Reference> {
        match self {
            Action::GetManifest { reference, .. }
            | Action::HeadManifest { reference, .. }
            | Action::PutManifest { reference, .. }
            | Action::DeleteManifest { reference, .. } => Some(reference),
            Action::UiAsset { .. }
            | Action::UiConfig
            | Action::Healthz
            | Action::Readyz
            | Action::Metrics
            | Action::ApiVersion
            | Action::ListCatalog { .. }
            | Action::ListTags { .. }
            | Action::StartUpload { .. }
            | Action::GetUpload { .. }
            | Action::PatchUpload { .. }
            | Action::PutUpload { .. }
            | Action::DeleteUpload { .. }
            | Action::GetBlob { .. }
            | Action::HeadBlob { .. }
            | Action::DeleteBlob { .. }
            | Action::GetReferrer { .. }
            | Action::ListRevisions { .. }
            | Action::ListUploads { .. }
            | Action::ListRepositories
            | Action::ListNamespaces { .. } => None,
        }
    }

    /// Returns `true` if this action writes registry state (uploads or manifest puts).
    ///
    /// Used to reject push operations against pull-through cache repositories.
    pub fn is_push(&self) -> bool {
        match self {
            Action::StartUpload { .. }
            | Action::PatchUpload { .. }
            | Action::PutUpload { .. }
            | Action::DeleteUpload { .. }
            | Action::PutManifest { .. } => true,
            Action::UiAsset { .. }
            | Action::UiConfig
            | Action::Healthz
            | Action::Readyz
            | Action::Metrics
            | Action::ApiVersion
            | Action::ListCatalog { .. }
            | Action::ListTags { .. }
            | Action::GetUpload { .. }
            | Action::GetBlob { .. }
            | Action::HeadBlob { .. }
            | Action::DeleteBlob { .. }
            | Action::GetManifest { .. }
            | Action::HeadManifest { .. }
            | Action::DeleteManifest { .. }
            | Action::GetReferrer { .. }
            | Action::ListRevisions { .. }
            | Action::ListUploads { .. }
            | Action::ListRepositories
            | Action::ListNamespaces { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests;
