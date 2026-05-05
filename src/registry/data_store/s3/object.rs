//! Object-level CRUD operations: reading, writing, deleting, and copying individual S3 objects.

use std::{
    fmt::Display,
    io::{Error as IoError, ErrorKind},
};

use aws_sdk_s3::{
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    types::{Delete, Error as S3TypesError, ObjectIdentifier},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::Backend;
use crate::registry::data_store::Error;

/// Result of a `get_object` request, exposing only what callers need without
/// leaking AWS SDK types. The body is a boxed `AsyncRead` and `content_length`
/// has already been validated as a non-negative `u64`.
pub struct GetObjectResult {
    pub body: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub content_length: u64,
}

const S3_ERROR_PRECONDITION_FAILED: &str = "PreconditionFailed";
const S3_ERROR_CONDITIONAL_REQUEST_CONFLICT: &str = "ConditionalRequestConflict";

fn is_conditional_write_conflict(error: &(impl ProvideErrorMetadata + Display)) -> bool {
    let code = error.code().unwrap_or_default();
    code == S3_ERROR_PRECONDITION_FAILED || code == S3_ERROR_CONDITIONAL_REQUEST_CONFLICT
}

fn classify_conditional_put_error(error: &(impl ProvideErrorMetadata + Display)) -> Error {
    if is_conditional_write_conflict(error) {
        Error::PreconditionFailed
    } else {
        Error::Io(error.to_string())
    }
}

pub fn aggregate_batch_delete_errors(errors: &[S3TypesError]) -> Option<IoError> {
    if errors.is_empty() {
        return None;
    }
    let msg = errors
        .iter()
        .filter_map(|e| e.message())
        .collect::<Vec<_>>()
        .join("; ");
    Some(IoError::other(format!("batch delete errors: {msg}")))
}

impl Backend {
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, IoError> {
        let (body, _) = self.read_with_etag(path).await?;
        Ok(body)
    }

    pub async fn read_with_etag(&self, path: &str) -> Result<(Vec<u8>, Option<String>), IoError> {
        let (body, etag, _) = self.read_with_metadata(path).await?;
        Ok((body, etag))
    }

    pub async fn read_with_metadata(
        &self,
        path: &str,
    ) -> Result<(Vec<u8>, Option<String>, Option<DateTime<Utc>>), IoError> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                let service_error = e.into_service_error();
                if service_error.is_no_such_key() {
                    IoError::new(ErrorKind::NotFound, "object not found")
                } else {
                    IoError::other(service_error.to_string())
                }
            });
        self.record_io_result(&result);
        let result = result?;

        let etag = result.e_tag;

        let last_modified = result
            .last_modified
            .and_then(|lm| DateTime::from_timestamp(lm.secs(), lm.subsec_nanos()));

        let body = result
            .body
            .collect()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok((body.into_bytes().to_vec(), etag, last_modified))
    }

    pub async fn delete(&self, path: &str) -> Result<(), IoError> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))
            .map(|_| ());
        self.record_io_result(&result);
        result
    }

    pub async fn delete_prefix(&self, prefix: &str) -> Result<(), IoError> {
        let full_prefix = self.full_key(prefix);
        let mut continuation_token = None;

        loop {
            let res = self
                .s3_client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix)
                .max_keys(1000)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|e| IoError::other(e.to_string()))?;

            let keys: Vec<ObjectIdentifier> = res
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|obj| obj.key.map(|k| ObjectIdentifier::builder().key(k).build()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| IoError::other(e.to_string()))?;

            self.delete_batch(keys).await?;

            if res.is_truncated.unwrap_or(false) {
                continuation_token = res.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn delete_batch(&self, objects: Vec<ObjectIdentifier>) -> Result<(), IoError> {
        if objects.is_empty() {
            return Ok(());
        }

        let delete = Delete::builder()
            .set_objects(Some(objects))
            .build()
            .map_err(|e| IoError::other(e.to_string()))?;

        let result = self
            .s3_client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        if let Some(err) = aggregate_batch_delete_errors(&result.errors.unwrap_or_default()) {
            return Err(err);
        }

        Ok(())
    }

    pub async fn object_size(&self, path: &str) -> Result<u64, IoError> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    IoError::new(ErrorKind::NotFound, "object not found")
                } else {
                    IoError::other(service_error.to_string())
                }
            });
        self.record_io_result(&result);
        let result = result?;

        u64::try_from(result.content_length.unwrap_or_default())
            .map_err(|e| IoError::other(format!("S3 returned negative content_length: {e}")))
    }

    pub async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<GetObjectResult, IoError> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);
        let mut req = self.s3_client.get_object().bucket(&self.bucket).key(&key);

        if let Some(offset) = offset {
            req = req.range(format!("bytes={offset}-"));
        }

        let result = req.send().await.map_err(|e| {
            let service_error = e.into_service_error();
            if service_error.is_no_such_key() {
                IoError::new(ErrorKind::NotFound, "object not found")
            } else {
                IoError::other(service_error.to_string())
            }
        });
        self.record_io_result(&result);
        let output = result?;

        let content_length = u64::try_from(output.content_length.unwrap_or_default())
            .map_err(|e| IoError::other(format!("S3 returned negative content_length: {e}")))?;

        Ok(GetObjectResult {
            body: Box::new(output.body.into_async_read()),
            content_length,
        })
    }

    pub async fn put_object_if_not_exists(
        &self,
        path: &str,
        data: impl Into<Bytes>,
    ) -> Result<Option<String>, Error> {
        self.check_circuit_breaker()
            .map_err(|e| Error::Io(e.to_string()))?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .if_none_match("*")
            .body(ByteStream::from(data.into()))
            .send()
            .await;

        let mapped = match result {
            Ok(output) => {
                let etag = output.e_tag().map(ToString::to_string);
                Ok(etag)
            }
            Err(e) => Err(classify_conditional_put_error(&e.into_service_error())),
        };
        self.record_data_result(&mapped);
        mapped
    }

    pub async fn put_object_if_match(
        &self,
        path: &str,
        etag: &str,
        data: impl Into<Bytes>,
    ) -> Result<Option<String>, Error> {
        self.check_circuit_breaker()
            .map_err(|e| Error::Io(e.to_string()))?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .if_match(etag)
            .body(ByteStream::from(data.into()))
            .send()
            .await;

        let mapped = match result {
            Ok(output) => {
                let etag = output.e_tag().map(ToString::to_string);
                Ok(etag)
            }
            Err(e) => Err(classify_conditional_put_error(&e.into_service_error())),
        };
        self.record_data_result(&mapped);
        mapped
    }

    pub async fn delete_if_match(&self, path: &str, etag: &str) -> Result<(), Error> {
        self.check_circuit_breaker()
            .map_err(|e| Error::Io(e.to_string()))?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .if_match(etag)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| {
                let service_err = e.into_service_error();
                if is_conditional_write_conflict(&service_err) {
                    Error::PreconditionFailed
                } else {
                    Error::Io(service_err.to_string())
                }
            });
        self.record_data_result(&result);
        result
    }

    pub async fn put_object(&self, path: &str, data: impl Into<Bytes>) -> Result<(), IoError> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(data.into()))
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))
            .map(|_| ());
        self.record_io_result(&result);
        result
    }

    pub async fn delete_object(&self, path: &str) -> Result<(), IoError> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))
            .map(|_| ());
        self.record_io_result(&result);
        result
    }

    pub async fn copy_object(&self, source: &str, destination: &str) -> Result<(), IoError> {
        let source_key = self.full_key(source);
        let destination_key = self.full_key(destination);

        self.s3_client
            .copy_object()
            .bucket(&self.bucket)
            .key(&destination_key)
            .copy_source(format!("{}/{}", self.bucket, source_key))
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok(())
    }

    pub async fn get_object_body(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<Vec<u8>, IoError> {
        let mut res = self.get_object(path, offset).await?;
        let mut buf = Vec::new();
        res.body.read_to_end(&mut buf).await?;
        Ok(buf)
    }
}
