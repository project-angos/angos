//! Multipart upload state machine: creating, uploading parts, completing, and aborting uploads.

use std::io::Error as IoError;

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::{Backend, channel_body::ChannelBody, s3_error_message};

/// A single part that has been successfully uploaded as part of a multipart upload.
///
/// `size` and `part_number` are stored as unsigned types even though the AWS
/// SDK reports them as `i64` and `i32` respectively. Byte counts and S3 part
/// numbers are unsigned by definition (S3 caps part numbers at `1..=10_000`).
/// Storing them as `u64` / `u32` lets consumers do arithmetic without casts.
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadedPart {
    pub part_number: u32,
    pub e_tag: String,
    pub size: u64,
}

/// Summary of a single in-progress multipart upload returned by [`Backend::list_multipart_uploads`].
#[derive(Debug)]
pub struct MultipartUpload {
    pub key: String,
    pub upload_id: String,
    pub initiated_at: DateTime<Utc>,
}

impl Backend {
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String, IoError> {
        let key = self.full_key(path);

        let res = self
            .s3_client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

        res.upload_id
            .ok_or_else(|| IoError::other("upload_id not found in response"))
    }

    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<String, IoError> {
        let key = self.full_key(path);
        let part_number = i32::try_from(part_number).map_err(|e| IoError::other(e.to_string()))?;

        let res = self
            .s3_client
            .upload_part()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

        Ok(res.e_tag.unwrap_or_default())
    }

    pub async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        upload_id: &str,
        part_number: u32,
        range: Option<String>,
    ) -> Result<String, IoError> {
        let source_key = self.full_key(source);
        let destination_key = self.full_key(destination);
        let part_number = i32::try_from(part_number).map_err(|e| IoError::other(e.to_string()))?;

        let mut req = self
            .s3_client
            .upload_part_copy()
            .bucket(&self.bucket)
            .key(&destination_key)
            .upload_id(upload_id)
            .part_number(part_number)
            .copy_source(format!("{}/{}", self.bucket, source_key));

        if let Some(range) = range {
            req = req.copy_source_range(range);
        }

        let response = req
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

        response
            .copy_part_result
            .and_then(|r| r.e_tag)
            .ok_or_else(|| IoError::other("e_tag not found in copy result"))
    }

    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<(), IoError> {
        let key = self.full_key(path);

        let completed_parts: Vec<CompletedPart> = parts
            .iter()
            .map(|p| {
                let part_number =
                    i32::try_from(p.part_number).map_err(|e| IoError::other(e.to_string()))?;
                Ok::<_, IoError>(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(&p.e_tag)
                        .build(),
                )
            })
            .collect::<Result<_, _>>()?;

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.s3_client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

        Ok(())
    }

    /// Streams a multipart part from an mpsc channel, internally wrapping the
    /// receiver in an SDK-compatible body. Callers stay free of AWS SDK types.
    pub async fn upload_part_streaming(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        content_length: u64,
        rx: mpsc::Receiver<Bytes>,
    ) -> Result<String, IoError> {
        let key = self.full_key(path);
        let part_number = i32::try_from(part_number).map_err(|e| IoError::other(e.to_string()))?;
        let content_length =
            i64::try_from(content_length).map_err(|e| IoError::other(e.to_string()))?;
        let body = ByteStream::from_body_1_x(ChannelBody { rx });

        let res = self
            .s3_client
            .upload_part()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(content_length)
            .body(body)
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

        Ok(res.e_tag.unwrap_or_default())
    }

    pub async fn abort_multipart_upload(&self, path: &str, upload_id: &str) -> Result<(), IoError> {
        let key = self.full_key(path);

        self.s3_client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

        Ok(())
    }

    pub async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<(Vec<MultipartUpload>, Option<String>, Option<String>), IoError> {
        let mut req = self.s3_client.list_multipart_uploads().bucket(&self.bucket);

        if let Some(prefix) = prefix {
            req = req.prefix(self.full_key(prefix));
        }
        if let Some(marker) = key_marker {
            req = req.key_marker(marker);
        }
        if let Some(marker) = upload_id_marker {
            req = req.upload_id_marker(marker);
        }

        let response = req
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

        let mut uploads = Vec::new();
        for upload in response.uploads.unwrap_or_default() {
            if let (Some(key), Some(upload_id), Some(initiated)) =
                (upload.key, upload.upload_id, upload.initiated)
            {
                let relative_key = if let Some(stripped) = key.strip_prefix(&self.key_prefix) {
                    stripped.trim_start_matches('/')
                } else {
                    &key
                };
                let initiated =
                    DateTime::from_timestamp(initiated.secs(), initiated.subsec_nanos())
                        .unwrap_or_else(Utc::now);
                uploads.push(MultipartUpload {
                    key: relative_key.to_string(),
                    upload_id,
                    initiated_at: initiated,
                });
            }
        }

        let (next_key_marker, next_upload_id_marker) = if response.is_truncated.unwrap_or(false) {
            (response.next_key_marker, response.next_upload_id_marker)
        } else {
            (None, None)
        };

        Ok((uploads, next_key_marker, next_upload_id_marker))
    }

    pub async fn search_multipart_upload_id(&self, path: &str) -> Result<Option<String>, IoError> {
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;

        loop {
            let (uploads, next_key, next_upload_id) = self
                .list_multipart_uploads(
                    Some(path),
                    key_marker.as_deref(),
                    upload_id_marker.as_deref(),
                )
                .await?;

            for upload in uploads {
                if upload.key == path {
                    return Ok(Some(upload.upload_id));
                }
            }

            if next_key.is_none() {
                break;
            }
            key_marker = next_key;
            upload_id_marker = next_upload_id;
        }

        Ok(None)
    }

    pub async fn abort_pending_uploads(&self, path: &str) -> Result<(), IoError> {
        while let Some(upload_id) = self.search_multipart_upload_id(path).await? {
            self.abort_multipart_upload(path, &upload_id).await?;
        }
        Ok(())
    }

    pub async fn list_parts(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<Vec<UploadedPart>, IoError> {
        let key = self.full_key(path);
        let mut parts = Vec::new();
        let mut part_number_marker = None;

        loop {
            let mut req = self
                .s3_client
                .list_parts()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(upload_id);

            if let Some(marker) = part_number_marker {
                req = req.part_number_marker(marker);
            }

            let response = req
                .send()
                .await
                .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

            for part in response.parts.unwrap_or_default() {
                if let (Some(part_number), Some(e_tag), Some(size)) =
                    (part.part_number, part.e_tag, part.size)
                {
                    let size = u64::try_from(size).map_err(|e| {
                        IoError::other(format!("S3 returned negative part size: {e}"))
                    })?;
                    let part_number = u32::try_from(part_number).map_err(|e| {
                        IoError::other(format!("S3 returned negative part number: {e}"))
                    })?;
                    parts.push(UploadedPart {
                        part_number,
                        e_tag,
                        size,
                    });
                }
            }

            if response.is_truncated.unwrap_or(false) {
                part_number_marker = response.next_part_number_marker;
            } else {
                break;
            }
        }

        Ok(parts)
    }
}
