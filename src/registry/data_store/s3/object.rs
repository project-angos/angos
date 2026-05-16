//! Object-level CRUD operations: reading, writing, deleting, and copying individual S3 objects.

use std::{
    fmt::Display,
    io::{Error as IoError, ErrorKind},
};

use aws_sdk_s3::{
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    types::{Delete, Error as S3TypesError, Object, ObjectIdentifier},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStreamExt, stream};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::registry::data_store::{
    Error,
    s3::{Backend, UploadedPart, s3_error_message},
};

/// Result of a `get_object` request, exposing only what callers need without
/// leaking AWS SDK types. The body is a boxed `AsyncRead` and `content_length`
/// has already been validated as a non-negative `u64`.
pub struct GetObjectResult {
    pub body: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub content_length: u64,
}

const S3_ERROR_PRECONDITION_FAILED: &str = "PreconditionFailed";
const S3_ERROR_CONDITIONAL_REQUEST_CONFLICT: &str = "ConditionalRequestConflict";
const MAX_MULTIPART_COPY_PARTS: u32 = 10_000;

struct CopyPartRange {
    part_number: u32,
    start: u64,
    end: u64,
}

fn is_conditional_write_conflict(error: &(impl ProvideErrorMetadata + Display)) -> bool {
    let code = error.code().unwrap_or_default();
    code == S3_ERROR_PRECONDITION_FAILED || code == S3_ERROR_CONDITIONAL_REQUEST_CONFLICT
}

fn classify_conditional_put_error(error: &(impl ProvideErrorMetadata + Display)) -> Error {
    if is_conditional_write_conflict(error) {
        Error::PreconditionFailed
    } else {
        Error::Io(s3_error_message(error))
    }
}

pub fn build_object_identifiers(contents: Vec<Object>) -> Result<Vec<ObjectIdentifier>, IoError> {
    contents
        .into_iter()
        .filter_map(|obj| obj.key.map(|k| ObjectIdentifier::builder().key(k).build()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| IoError::other(e.to_string()))
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

fn copy_part_ranges(size: u64, chunk_size: u64) -> Result<Vec<CopyPartRange>, IoError> {
    if chunk_size == 0 {
        return Err(IoError::other(
            "multipart copy chunk size must be greater than 0",
        ));
    }

    let mut ranges = Vec::new();
    let mut start = 0;
    let mut part_number = 1;

    while start < size {
        if part_number > MAX_MULTIPART_COPY_PARTS {
            return Err(IoError::other(format!(
                "multipart copy requires more than {MAX_MULTIPART_COPY_PARTS} parts"
            )));
        }

        let end = start.saturating_add(chunk_size).min(size) - 1;
        ranges.push(CopyPartRange {
            part_number,
            start,
            end,
        });
        start = end + 1;
        part_number += 1;
    }

    Ok(ranges)
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
                    IoError::other(s3_error_message(&service_error))
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
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))
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
                .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

            let keys = build_object_identifiers(res.contents.unwrap_or_default())?;

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
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))?;

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
                    IoError::other(s3_error_message(&service_error))
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
                IoError::other(s3_error_message(&service_error))
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
                    Error::Io(s3_error_message(&service_err))
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
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))
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
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))
            .map(|_| ());
        self.record_io_result(&result);
        result
    }

    pub async fn copy_object(&self, source: &str, destination: &str) -> Result<(), IoError> {
        let source_size = self.object_size(source).await?;
        if source_size <= self.multipart_copy_threshold {
            return self.copy_object_single(source, destination).await;
        }

        self.copy_object_multipart(source, destination, source_size)
            .await
    }

    async fn copy_object_single(&self, source: &str, destination: &str) -> Result<(), IoError> {
        let source_key = self.full_key(source);
        let destination_key = self.full_key(destination);

        let result = self
            .s3_client
            .copy_object()
            .bucket(&self.bucket)
            .key(&destination_key)
            .copy_source(format!("{}/{}", self.bucket, source_key))
            .send()
            .await
            .map_err(|e| IoError::other(s3_error_message(&e.into_service_error())))
            .map(|_| ());
        self.record_io_result(&result);
        result
    }

    async fn copy_object_multipart(
        &self,
        source: &str,
        destination: &str,
        source_size: u64,
    ) -> Result<(), IoError> {
        let upload_id = self.create_multipart_upload(destination).await?;
        let result = match self
            .copy_object_multipart_parts(source, destination, &upload_id, source_size)
            .await
        {
            Ok(parts) => {
                self.complete_multipart_upload(destination, &upload_id, &parts)
                    .await
            }
            Err(e) => Err(e),
        };

        if result.is_err() {
            let _ = self.abort_multipart_upload(destination, &upload_id).await;
        }

        result
    }

    async fn copy_object_multipart_parts(
        &self,
        source: &str,
        destination: &str,
        upload_id: &str,
        source_size: u64,
    ) -> Result<Vec<UploadedPart>, IoError> {
        let ranges = copy_part_ranges(source_size, self.multipart_copy_chunk_size)?;
        let mut parts = stream::iter(ranges)
            .map(|range| async move {
                let copy_range = format!("bytes={}-{}", range.start, range.end);
                let e_tag = self
                    .upload_part_copy(
                        source,
                        destination,
                        upload_id,
                        range.part_number,
                        Some(copy_range),
                    )
                    .await?;

                Ok::<_, IoError>(UploadedPart {
                    part_number: range.part_number,
                    e_tag,
                    size: range.end - range.start + 1,
                })
            })
            .buffer_unordered(self.multipart_copy_jobs.max(1))
            .try_collect::<Vec<_>>()
            .await?;

        parts.sort_by_key(|part| part.part_number);
        Ok(parts)
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

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use crate::{
        registry::data_store::s3::{Backend, BackendConfig, object::copy_part_ranges},
        secret::Secret,
    };

    const MIB: u64 = 1024 * 1024;

    fn test_config(endpoint: String) -> BackendConfig {
        BackendConfig {
            access_key_id: Secret::new("key".to_string()),
            secret_key: Secret::new("secret".to_string()),
            endpoint,
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            multipart_copy_threshold: ByteSize::mib(5),
            multipart_copy_chunk_size: ByteSize::mib(5),
            multipart_copy_jobs: 2,
            ..Default::default()
        }
    }

    fn create_multipart_upload_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_string(
            r#"<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Bucket>test-bucket</Bucket>
                <Key>destination</Key>
                <UploadId>copy-upload</UploadId>
            </InitiateMultipartUploadResult>"#,
        )
    }

    fn upload_part_copy_response(part: u32) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_string(format!(
            r#"<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <LastModified>2026-05-13T00:00:00.000Z</LastModified>
                <ETag>"etag-{part}"</ETag>
            </CopyPartResult>"#
        ))
    }

    fn complete_multipart_upload_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_string(
            r#"<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Location>http://localhost/test-bucket/destination</Location>
                <Bucket>test-bucket</Bucket>
                <Key>destination</Key>
                <ETag>"complete"</ETag>
            </CompleteMultipartUploadResult>"#,
        )
    }

    #[test]
    fn copy_part_ranges_covers_source_without_gaps() {
        let ranges = copy_part_ranges(12 * MIB, 5 * MIB).unwrap();

        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].part_number, 1);
        assert_eq!((ranges[0].start, ranges[0].end), (0, 5 * MIB - 1));
        assert_eq!(ranges[1].part_number, 2);
        assert_eq!((ranges[1].start, ranges[1].end), (5 * MIB, 10 * MIB - 1));
        assert_eq!(ranges[2].part_number, 3);
        assert_eq!((ranges[2].start, ranges[2].end), (10 * MIB, 12 * MIB - 1));
    }

    #[tokio::test]
    async fn copy_object_uses_multipart_copy_above_threshold() {
        let server = MockServer::start().await;
        let source_size = 12 * MIB;

        Mock::given(method("HEAD"))
            .and(path("/test-bucket/source"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", source_size))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/test-bucket/destination"))
            .and(query_param("uploads", ""))
            .respond_with(create_multipart_upload_response())
            .mount(&server)
            .await;

        for part_number in 1..=3 {
            Mock::given(method("PUT"))
                .and(path("/test-bucket/destination"))
                .and(query_param("partNumber", part_number.to_string()))
                .and(query_param("uploadId", "copy-upload"))
                .respond_with(upload_part_copy_response(part_number))
                .mount(&server)
                .await;
        }

        Mock::given(method("POST"))
            .and(path("/test-bucket/destination"))
            .and(query_param("uploadId", "copy-upload"))
            .respond_with(complete_multipart_upload_response())
            .mount(&server)
            .await;

        let backend = Backend::new(&test_config(server.uri())).unwrap();
        let result = backend.copy_object("source", "destination").await;

        let requests = server.received_requests().await.unwrap();
        assert!(
            result.is_ok(),
            "copy_object failed: {result:?}; requests: {requests:#?}"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.method.as_str() == "HEAD")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.url.query() == Some("uploads"))
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.headers.get("x-amz-copy-source-range").is_some())
                .count(),
            3
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| {
                    r.method.as_str() == "PUT"
                        && r.url.path() == "/test-bucket/destination"
                        && r.url.query().is_none()
                })
                .count(),
            0
        );
    }
}
