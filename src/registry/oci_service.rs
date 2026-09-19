//! [`OciService`] for [`Registry`]: the spec's operations bound to the
//! registry's implementation.
//!
//! The trait is the seam the transport dispatches through; the inherent methods
//! (still called directly by tests and internal jobs) hold the behaviour. Each
//! method here adapts the trait's uniform `&EventActor` to the inherent
//! `Option<EventActor>` and its `BoxedReader` body to the inherent generic
//! stream, and returns the typed response the transport renders.

use async_trait::async_trait;

use angos_oci::Namespace;
use angos_oci::request::{
    CompleteUploadRequest, DeleteBlobRequest, DeleteManifestRequest, DeleteUploadRequest,
    GetBlobRequest, GetManifestRequest, GetReferrersRequest, GetUploadRequest, HeadBlobRequest,
    HeadManifestRequest, ListTagsRequest, MountBlobRequest, PatchUploadRequest, PutManifestRequest,
    StartUploadRequest,
};
use angos_oci_service::{
    Accepted, ApiVersion, BlobDescriptor, BlobGet, BlobWritten, ManifestDescriptor, ManifestGet,
    ManifestWritten, NoContent, OciService, Referrers, StartUpload, Tags, UploadSession,
};
use angos_storage::BoxedReader;

use crate::event_webhook::event::EventActor;
use crate::registry::{Error, Registry, api_version};

#[async_trait]
impl OciService for Registry {
    type Actor = EventActor;
    type Body = BoxedReader;
    type Error = Error;

    async fn check_version(&self, _actor: &EventActor) -> Result<ApiVersion, Error> {
        Ok(api_version())
    }

    async fn get_manifest(
        &self,
        actor: &EventActor,
        request: GetManifestRequest,
        allow_redirect: bool,
    ) -> Result<ManifestGet, Error> {
        self.handle_get_manifest(Some(actor.clone()), request, allow_redirect)
            .await
    }

    async fn head_manifest(
        &self,
        actor: &EventActor,
        request: HeadManifestRequest,
    ) -> Result<ManifestDescriptor, Error> {
        self.handle_head_manifest(Some(actor.clone()), request)
            .await
    }

    async fn put_manifest(
        &self,
        actor: &EventActor,
        request: PutManifestRequest,
        body: BoxedReader,
    ) -> Result<ManifestWritten, Error> {
        self.handle_put_manifest(Some(actor.clone()), request, body)
            .await
    }

    async fn delete_manifest(
        &self,
        actor: &EventActor,
        request: DeleteManifestRequest,
    ) -> Result<Accepted, Error> {
        self.handle_delete_manifest(Some(actor.clone()), request)
            .await
    }

    async fn get_blob(
        &self,
        actor: &EventActor,
        request: GetBlobRequest,
        allow_redirect: bool,
    ) -> Result<BlobGet<BoxedReader>, Error> {
        self.handle_get_blob(Some(actor.clone()), request, allow_redirect)
            .await
    }

    async fn head_blob(
        &self,
        _actor: &EventActor,
        request: HeadBlobRequest,
    ) -> Result<BlobDescriptor, Error> {
        self.handle_head_blob(request).await
    }

    async fn delete_blob(
        &self,
        _actor: &EventActor,
        request: DeleteBlobRequest,
    ) -> Result<Accepted, Error> {
        self.handle_delete_blob(request).await
    }

    async fn start_upload(
        &self,
        actor: &EventActor,
        request: StartUploadRequest,
        body: BoxedReader,
    ) -> Result<StartUpload, Error> {
        self.handle_start_upload(Some(actor.clone()), request, body)
            .await
    }

    async fn mount_blob(
        &self,
        actor: &EventActor,
        request: MountBlobRequest,
        source: Option<Namespace>,
    ) -> Result<StartUpload, Error> {
        self.handle_mount_blob(Some(actor.clone()), request, source)
            .await
    }

    async fn upload_status(
        &self,
        _actor: &EventActor,
        request: GetUploadRequest,
    ) -> Result<UploadSession, Error> {
        self.handle_upload_status(request).await
    }

    async fn patch_upload(
        &self,
        _actor: &EventActor,
        request: PatchUploadRequest,
        body: BoxedReader,
    ) -> Result<UploadSession, Error> {
        self.handle_patch_upload(request, body).await
    }

    async fn complete_upload(
        &self,
        actor: &EventActor,
        request: CompleteUploadRequest,
        body: BoxedReader,
    ) -> Result<BlobWritten, Error> {
        self.handle_complete_upload(Some(actor.clone()), request, body)
            .await
    }

    async fn cancel_upload(
        &self,
        _actor: &EventActor,
        request: DeleteUploadRequest,
    ) -> Result<NoContent, Error> {
        self.handle_cancel_upload(request).await
    }

    async fn list_tags(
        &self,
        _actor: &EventActor,
        request: ListTagsRequest,
    ) -> Result<Tags, Error> {
        self.handle_list_tags(request).await
    }

    async fn get_referrers(
        &self,
        _actor: &EventActor,
        request: GetReferrersRequest,
    ) -> Result<Referrers, Error> {
        self.handle_get_referrers(request).await
    }
}
