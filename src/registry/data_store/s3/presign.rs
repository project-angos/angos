//! Presigned URL generation for time-limited, unauthenticated S3 object access.

use std::{io::Error as IoError, time::Duration};

use aws_sdk_s3::presigning::PresigningConfig;

use super::Backend;

impl Backend {
    pub async fn generate_presigned_url(
        &self,
        path: &str,
        expires_in: Duration,
        response_content_type: Option<&str>,
    ) -> Result<String, IoError> {
        let key = self.full_key(path);

        let mut builder = self.s3_client.get_object().bucket(&self.bucket).key(&key);

        if let Some(ct) = response_content_type {
            builder = builder.response_content_type(ct);
        }

        let presigned = builder
            .presigned(
                PresigningConfig::expires_in(expires_in)
                    .map_err(|e| IoError::other(e.to_string()))?,
            )
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok(presigned.uri().to_string())
    }
}
