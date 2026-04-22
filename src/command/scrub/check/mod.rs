mod blob;
mod link_ops;
mod link_references;
mod manifest;
mod media_type;
mod multipart;
mod retention;
mod tag;
mod upload;

use async_trait::async_trait;
pub use blob::BlobChecker;
pub use link_ops::ensure_link;
pub use link_references::LinkReferencesChecker;
pub use manifest::ManifestChecker;
pub use media_type::MediaTypeChecker;
pub use multipart::MultipartChecker;
pub use retention::RetentionChecker;
pub use tag::TagChecker;
pub use upload::UploadChecker;

use crate::registry::Error;

#[async_trait]
pub trait NamespaceChecker: Send + Sync {
    async fn check_namespace(&self, namespace: &str) -> Result<(), Error>;
}
