use std::time::Duration;

use async_trait::async_trait;

use crate::error::Error;

/// Capability for issuing presigned download URLs.
///
/// Currently only the S3 backend implements this; FS exposes no presigned-URL
/// concept. Consumers that need presigning hold an `Option<Arc<dyn PresignedStore>>`
/// and gracefully degrade to streaming reads when the capability is absent.
#[async_trait]
pub trait PresignedStore: Send + Sync {
    /// Generate a signed download URL for `key` that expires after `ttl`.
    /// `content_type`, when provided, is bound into the signature so the
    /// served response carries that `Content-Type` header.
    async fn presign_get(
        &self,
        key: &str,
        ttl: Duration,
        content_type: Option<&str>,
    ) -> Result<String, Error>;
}
