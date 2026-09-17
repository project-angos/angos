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
        }
    }
}

impl From<layer::Listing> for ext::LayerListing {
    fn from(listing: layer::Listing) -> Self {
        ext::LayerListing {
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
        visibility: &dyn ext::NamespaceVisibility,
    ) -> Result<ext::RepositoriesBody, Error> {
        self.get_repositories_info(visibility).await
    }

    async fn list_namespaces(
        &self,
        repository: Namespace,
        visibility: &dyn ext::NamespaceVisibility,
    ) -> Result<ext::NamespacesBody, Error> {
        self.get_namespaces_info(&repository, visibility).await
    }

    async fn list_revisions(&self, namespace: Namespace) -> Result<ext::RevisionsBody, Error> {
        self.get_revisions_info(&namespace).await
    }

    async fn list_uploads(&self, namespace: Namespace) -> Result<ext::UploadsBody, Error> {
        self.get_uploads_info(&namespace).await
    }

    async fn list_pulls(&self, request: ext::ListPullsRequest) -> Result<ext::PullsBody, Error> {
        self.get_pull_history(request).await
    }

    async fn list_layer_entries(
        &self,
        request: ext::LayerEntriesRequest,
    ) -> Result<ext::LayerEntries, Error> {
        self.get_layer_entries(request).await
    }

    async fn get_layer_file(
        &self,
        request: ext::LayerFileRequest,
    ) -> Result<ext::LayerFile<Self::Body>, Error> {
        self.get_layer_file(request).await
    }

    async fn list_jobs(&self, request: ext::ListJobsRequest) -> Result<ext::JobsBody, Error> {
        self.get_jobs_info(request).await
    }

    async fn list_failed_jobs(
        &self,
        request: ext::ListJobsRequest,
    ) -> Result<ext::FailedJobsBody, Error> {
        self.get_failed_jobs_info(request).await
    }

    async fn retry_job(&self, request: ext::RetryJobRequest) -> Result<ext::NoContent, Error> {
        self.retry_failed_job(request).await
    }

    async fn delete_job(&self, request: ext::DeleteJobRequest) -> Result<ext::NoContent, Error> {
        self.delete_job(request).await
    }
}
