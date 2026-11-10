use std::time::Duration;

use async_trait::async_trait;

use crate::error::Error;

/// Presigned download URLs, which only S3 offers; callers fall back to
/// streaming reads without it.
#[async_trait]
pub trait PresignedStore: Send + Sync {
    /// `content_type` is bound into the signature, so the served response
    /// carries it.
    async fn presign_get(
        &self,
        key: &str,
        ttl: Duration,
        content_type: Option<&str>,
    ) -> Result<String, Error>;
}
