//! [`AngosExtensionService`] for [`Registry`]: the `_angos/` endpoints, none of
//! them in the OCI or Docker spec.
//!
//! The trait is the transport's seam; the inherent methods on [`Registry`] hold
//! the behaviour and this delegates to them. The job-administration vocabulary
//! ([`ext::Queue`]/[`ext::JobState`]) is the extension's own, so it converts
//! into the job engine's [`jobs::Queue`]/[`jobs::JobState`] at this edge.

use async_trait::async_trait;

use angos_extension_service as ext;
use angos_oci::Namespace;

use crate::jobs;
use crate::layer;
use crate::registry::layers::LayerFileReader;
use crate::registry::{Error, Registry};

impl From<ext::Queue> for jobs::Queue {
    fn from(queue: ext::Queue) -> Self {
        match queue {
            ext::Queue::Cache => jobs::Queue::Cache,
            ext::Queue::Replication => jobs::Queue::Replication,
            ext::Queue::Scan => jobs::Queue::Scan,
            ext::Queue::Index => jobs::Queue::Index,
        }
    }
}

impl From<ext::JobState> for jobs::JobState {
    fn from(state: ext::JobState) -> Self {
        match state {
            ext::JobState::Pending => jobs::JobState::Pending,
            ext::JobState::Failed => jobs::JobState::Failed,
        }
    }
}

impl From<layer::Kind> for ext::EntryKind {
    fn from(kind: layer::Kind) -> Self {
        match kind {
            layer::Kind::File => ext::EntryKind::File,
            layer::Kind::Dir => ext::EntryKind::Dir,
            layer::Kind::Symlink => ext::EntryKind::Symlink,
            layer::Kind::Hardlink => ext::EntryKind::Hardlink,
            layer::Kind::Whiteout => ext::EntryKind::Whiteout,
            layer::Kind::Opaque => ext::EntryKind::Opaque,
            layer::Kind::Other => ext::EntryKind::Other,
        }
    }
}

impl From<layer::SecretKind> for ext::SecretKind {
    fn from(kind: layer::SecretKind) -> Self {
        match kind {
            layer::SecretKind::PrivateKey => ext::SecretKind::PrivateKey,
            layer::SecretKind::AwsCredentials => ext::SecretKind::AwsCredentials,
            layer::SecretKind::RegistryAuth => ext::SecretKind::RegistryAuth,
            layer::SecretKind::NpmToken => ext::SecretKind::NpmToken,
            layer::SecretKind::GitCredentials => ext::SecretKind::GitCredentials,
            layer::SecretKind::Netrc => ext::SecretKind::Netrc,
            layer::SecretKind::GithubToken => ext::SecretKind::GithubToken,
            layer::SecretKind::GitlabToken => ext::SecretKind::GitlabToken,
            layer::SecretKind::SlackToken => ext::SecretKind::SlackToken,
            layer::SecretKind::StripeKey => ext::SecretKind::StripeKey,
            layer::SecretKind::AwsAccessKey => ext::SecretKind::AwsAccessKey,
            layer::SecretKind::Kubeconfig => ext::SecretKind::Kubeconfig,
        }
    }
}

impl From<layer::Secret> for ext::Secret {
    fn from(secret: layer::Secret) -> Self {
        ext::Secret {
            kind: secret.kind.into(),
            line: secret.line,
        }
    }
}

impl From<layer::Entry> for ext::LayerEntry {
    fn from(entry: layer::Entry) -> Self {
        ext::LayerEntry {
            path: entry.path,
            kind: entry.kind.into(),
            size: entry.size,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            mtime: entry.mtime,
            link: entry.link,
            offset: entry.offset,
            content: entry.content.map(|content| ext::FileContent {
                sha256: content.sha256,
                sha512: content.sha512,
                mime_type: content.mime_type,
                secrets: content.secrets.into_iter().map(Into::into).collect(),
            }),
            capabilities: entry.capabilities,
        }
    }
}

impl From<layer::Listing> for ext::LayerListing {
    fn from(listing: layer::Listing) -> Self {
        ext::LayerListing {
            refreshing: listing.is_outdated(),
            compressed: listing.compressed,
            uncompressed_size: listing.uncompressed_size,
            entries: listing.entries.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl ext::AngosExtensionService for Registry {
    type Body = LayerFileReader;
    type Error = Error;

    async fn list_repositories(
        &self,
        order: ext::SortOrder,
        page: ext::PageRequest,
        visibility: &dyn ext::NamespaceVisibility,
    ) -> Result<ext::RepositoriesBody, Error> {
        self.handle_list_repositories(order, page, visibility).await
    }

    async fn list_namespaces(
        &self,
        request: ext::ListNamespacesRequest,
        visibility: &dyn ext::NamespaceVisibility,
    ) -> Result<ext::NamespacesBody, Error> {
        self.handle_list_namespaces(request, visibility).await
    }

    async fn list_revisions(
        &self,
        namespace: Namespace,
        selection: ext::RevisionSelection,
    ) -> Result<ext::RevisionsBody, Error> {
        self.handle_list_revisions(&namespace, selection).await
    }

    async fn list_uploads(
        &self,
        namespace: Namespace,
        page: ext::PageRequest,
    ) -> Result<ext::UploadsBody, Error> {
        self.handle_list_uploads(&namespace, page).await
    }

    async fn list_pulls(&self, request: ext::ListPullsRequest) -> Result<ext::PullsBody, Error> {
        self.handle_list_pulls(request).await
    }

    async fn list_layer_entries(
        &self,
        request: ext::LayerEntriesRequest,
    ) -> Result<ext::LayerEntries, Error> {
        self.handle_list_layer_entries(request).await
    }

    async fn get_layer_file(
        &self,
        request: ext::LayerFileRequest,
    ) -> Result<ext::LayerFile<Self::Body>, Error> {
        self.handle_get_layer_file(request).await
    }

    async fn get_layer_file_details(
        &self,
        request: ext::LayerFileDetailsRequest,
    ) -> Result<ext::LayerFileDetails, Error> {
        self.handle_get_layer_file_details(request).await
    }

    async fn list_jobs(&self, request: ext::ListJobsRequest) -> Result<ext::JobsBody, Error> {
        self.handle_list_jobs(request).await
    }

    async fn list_failed_jobs(
        &self,
        request: ext::ListJobsRequest,
    ) -> Result<ext::FailedJobsBody, Error> {
        self.handle_list_failed_jobs(request).await
    }

    async fn retry_job(&self, request: ext::RetryJobRequest) -> Result<ext::NoContent, Error> {
        self.handle_retry_job(request).await
    }

    async fn delete_job(&self, request: ext::DeleteJobRequest) -> Result<ext::NoContent, Error> {
        self.handle_delete_job(request).await
    }
}
