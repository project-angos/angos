use std::num::NonZeroU16;

use serde::Serialize;

use angos_extension_service::JobKey;
use angos_oci::request::ManifestPutTarget;
use angos_oci::{Algorithm, Digest, MediaType, Namespace, Reference, UploadSessionId};

use crate::jobs::{JobState, Queue};

/// A parsed HTTP request: the operation CEL policies see and the route the
/// dispatcher follows. Serializes to the flat `request` object of
/// `doc/reference/cel-expressions.md`; variants sharing a `rename` (HEAD and
/// GET, say) route differently but look the same to policies.
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
    #[serde(rename = "get-token")]
    Token,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        digest_algorithm: Option<Algorithm>,
    },
    /// Cross-repository blob mount (`POST .../blobs/uploads/?mount=<digest>`).
    /// A distinct CEL action from `start-upload` so policies can gate mounts
    /// separately.
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
        session_id: UploadSessionId,
    },
    #[serde(rename = "update-upload")]
    PatchUpload {
        namespace: Namespace,
        session_id: UploadSessionId,
    },
    #[serde(rename = "complete-upload")]
    PutUpload {
        namespace: Namespace,
        digest: Digest,
        session_id: UploadSessionId,
    },
    #[serde(rename = "cancel-upload")]
    DeleteUpload {
        namespace: Namespace,
        session_id: UploadSessionId,
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
    /// The filesystem listing of a layer, derived from the blob and read under
    /// the blob's own action name and ownership check.
    #[serde(rename = "get-blob")]
    ListLayerEntries {
        namespace: Namespace,
        digest: Digest,
    },
    /// One file out of a layer, by its path in the listing.
    #[serde(rename = "get-blob")]
    GetLayerFile {
        namespace: Namespace,
        digest: Digest,
        path: String,
        download: bool,
    },
    /// What one file of a layer holds once decoded, read like the file.
    #[serde(rename = "get-blob")]
    GetLayerFileDetails {
        namespace: Namespace,
        digest: Digest,
        path: String,
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
        #[serde(flatten)]
        target: ManifestPutTarget,
    },
    #[serde(rename = "delete-manifest")]
    DeleteManifest {
        namespace: Namespace,
        reference: Reference,
    },
    #[serde(rename = "get-referrers")]
    /// `artifact_type` is the validated `?artifactType=` filter; a malformed
    /// value is refused by the router rather than reaching the listing.
    GetReferrer {
        namespace: Namespace,
        digest: Digest,
        #[serde(skip_serializing_if = "Option::is_none")]
        artifact_type: Option<MediaType>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last: Option<String>,
    },
    #[serde(rename = "list-revisions")]
    ListRevisions {
        namespace: Namespace,
    },
    #[serde(rename = "list-uploads")]
    ListUploads {
        namespace: Namespace,
    },
    /// Same CEL action name as `ListRevisions`: pull history is the same
    /// per-namespace revision metadata, read one target at a time.
    #[serde(rename = "list-revisions")]
    ListPulls {
        namespace: Namespace,
        reference: Reference,
    },
    #[serde(rename = "list-repositories")]
    ListRepositories,
    #[serde(rename = "list-namespaces")]
    ListNamespaces {
        repository: Namespace,
    },
    /// List pending/in-flight durable jobs. Distinct action name so operators
    /// can gate job administration behind higher privilege than registry reads.
    /// `queue` is exposed to CEL so a policy can gate replication-queue
    /// administration separately.
    #[serde(rename = "list-jobs")]
    ListJobs {
        queue: Queue,
        #[serde(skip_serializing_if = "Option::is_none")]
        n: Option<NonZeroU16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<String>,
    },
    #[serde(rename = "list-failed-jobs")]
    ListFailedJobs {
        queue: Queue,
        #[serde(skip_serializing_if = "Option::is_none")]
        n: Option<NonZeroU16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<String>,
    },
    /// Requeue a dead-letter job. `queue` is exposed to CEL; `storage_key` is
    /// HTTP routing only, excluded from the CEL payload like other addressing
    /// fields.
    #[serde(rename = "retry-job")]
    RetryJob {
        queue: Queue,
        #[serde(skip)]
        storage_key: JobKey,
    },
    #[serde(rename = "delete-job")]
    DeleteJob {
        queue: Queue,
        #[serde(skip)]
        state: JobState,
        #[serde(skip)]
        storage_key: JobKey,
    },
}

#[derive(Default)]
struct ActionData<'a> {
    namespace: Option<&'a Namespace>,
    digest: Option<&'a Digest>,
    is_push: bool,
}

impl Action {
    /// The CEL action name, also sent as the `X-Registry-Action` webhook header.
    pub fn action_name(&self) -> &'static str {
        match self {
            Action::UiAsset { .. } => "ui-asset",
            Action::UiConfig => "ui-config",
            Action::Token => "get-token",
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
            Action::GetBlob { .. }
            | Action::HeadBlob { .. }
            | Action::ListLayerEntries { .. }
            | Action::GetLayerFile { .. }
            | Action::GetLayerFileDetails { .. } => "get-blob",
            Action::DeleteBlob { .. } => "delete-blob",
            Action::GetManifest { .. } | Action::HeadManifest { .. } => "get-manifest",
            Action::PutManifest { .. } => "put-manifest",
            Action::DeleteManifest { .. } => "delete-manifest",
            Action::GetReferrer { .. } => "get-referrers",
            Action::ListRevisions { .. } | Action::ListPulls { .. } => "list-revisions",
            Action::ListUploads { .. } => "list-uploads",
            Action::ListRepositories => "list-repositories",
            Action::ListNamespaces { .. } => "list-namespaces",
            Action::ListJobs { .. } => "list-jobs",
            Action::ListFailedJobs { .. } => "list-failed-jobs",
            Action::RetryJob { .. } => "retry-job",
            Action::DeleteJob { .. } => "delete-job",
        }
    }

    fn action_data(&self) -> ActionData<'_> {
        match self {
            Action::UiAsset { .. }
            | Action::UiConfig
            | Action::Token
            | Action::Healthz
            | Action::Readyz
            | Action::Metrics
            | Action::ApiVersion
            | Action::ListCatalog { .. }
            | Action::ListRepositories
            | Action::ListNamespaces { .. }
            | Action::ListJobs { .. }
            | Action::ListFailedJobs { .. } => ActionData::default(),

            // Job mutations carry no namespace/digest, but flag `is_push` so they
            // are treated as state-changing for any write-sensitive policy logic.
            Action::RetryJob { .. } | Action::DeleteJob { .. } => ActionData {
                is_push: true,
                ..ActionData::default()
            },

            Action::ListTags { namespace, .. }
            | Action::GetUpload { namespace, .. }
            | Action::ListRevisions { namespace }
            | Action::ListUploads { namespace }
            | Action::ListPulls { namespace, .. }
            | Action::GetManifest { namespace, .. }
            | Action::HeadManifest { namespace, .. }
            | Action::DeleteManifest { namespace, .. } => ActionData {
                namespace: Some(namespace),
                ..ActionData::default()
            },

            Action::PatchUpload { namespace, .. } | Action::DeleteUpload { namespace, .. } => {
                ActionData {
                    namespace: Some(namespace),
                    is_push: true,
                    ..ActionData::default()
                }
            }

            Action::StartUpload {
                namespace, digest, ..
            } => ActionData {
                namespace: Some(namespace),
                digest: digest.as_ref(),
                is_push: true,
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
            },

            Action::GetBlob { namespace, digest }
            | Action::HeadBlob { namespace, digest }
            | Action::ListLayerEntries { namespace, digest }
            | Action::GetLayerFile {
                namespace, digest, ..
            }
            | Action::GetLayerFileDetails {
                namespace, digest, ..
            }
            | Action::DeleteBlob { namespace, digest }
            | Action::GetReferrer {
                namespace, digest, ..
            } => ActionData {
                namespace: Some(namespace),
                digest: Some(digest),
                ..ActionData::default()
            },

            Action::PutManifest { namespace, .. } => ActionData {
                namespace: Some(namespace),
                is_push: true,
                ..ActionData::default()
            },
        }
    }

    pub fn namespace(&self) -> Option<&Namespace> {
        self.action_data().namespace
    }

    pub fn digest(&self) -> Option<&Digest> {
        self.action_data().digest
    }

    pub fn reference(&self) -> Option<Reference> {
        match self {
            Action::GetManifest { reference, .. }
            | Action::HeadManifest { reference, .. }
            | Action::DeleteManifest { reference, .. } => Some(reference.clone()),
            Action::PutManifest { target, .. } => Some(target.reference()),
            _ => None,
        }
    }

    /// Whether this action writes registry state (uploads, manifest puts, job mutations).
    pub fn is_push(&self) -> bool {
        self.action_data().is_push
    }
}

#[cfg(test)]
mod tests;
