use std::{
    io::{Error as IoError, ErrorKind},
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use aws_sdk_s3::{
    Client as S3Client, Config as S3Config,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
    error::ProvideErrorMetadata,
    operation::get_object::GetObjectOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier},
};
use bytes::Bytes;
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tracing::warn;

use crate::registry::data_store::Error;

const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
const CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 10;

#[derive(Clone, Debug)]
struct CircuitBreaker {
    consecutive_failures: Arc<AtomicU32>,
    opened_at_epoch_secs: Arc<AtomicU64>,
}

impl CircuitBreaker {
    fn new() -> Self {
        Self {
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            opened_at_epoch_secs: Arc::new(AtomicU64::new(0)),
        }
    }

    fn check(&self) -> Result<(), IoError> {
        let failures = self.consecutive_failures.load(Ordering::Acquire);
        if failures < CIRCUIT_BREAKER_THRESHOLD {
            return Ok(());
        }
        let opened_at = self.opened_at_epoch_secs.load(Ordering::Acquire);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now.saturating_sub(opened_at) >= CIRCUIT_BREAKER_COOLDOWN_SECS {
            // Half-open: allow one probe request through
            return Ok(());
        }
        Err(IoError::other(format!(
            "S3 circuit breaker open: {failures} consecutive failures, \
             cooling down for {CIRCUIT_BREAKER_COOLDOWN_SECS}s"
        )))
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
    }

    fn record_failure(&self) {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::AcqRel);
        if prev + 1 == CIRCUIT_BREAKER_THRESHOLD {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.opened_at_epoch_secs.store(now, Ordering::Release);
            warn!(
                threshold = CIRCUIT_BREAKER_THRESHOLD,
                cooldown_secs = CIRCUIT_BREAKER_COOLDOWN_SECS,
                "S3 circuit breaker opened after {CIRCUIT_BREAKER_THRESHOLD} consecutive failures"
            );
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct BackendConfig {
    pub access_key_id: String,
    pub secret_key: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
    pub multipart_copy_threshold: ByteSize,
    pub multipart_copy_chunk_size: ByteSize,
    pub multipart_copy_jobs: usize,
    pub multipart_part_size: ByteSize,
    pub multipart_uniform_parts: bool,
    pub operation_timeout_secs: u64,
    pub operation_attempt_timeout_secs: u64,
    pub max_attempts: u32,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            access_key_id: String::new(),
            secret_key: String::new(),
            endpoint: String::new(),
            bucket: String::new(),
            region: String::new(),
            key_prefix: String::new(),
            multipart_copy_threshold: ByteSize::gb(5),
            multipart_copy_chunk_size: ByteSize::mb(100),
            multipart_copy_jobs: 4,
            multipart_part_size: ByteSize::mib(50),
            multipart_uniform_parts: false,
            operation_timeout_secs: 900,
            operation_attempt_timeout_secs: 300,
            max_attempts: 3,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Backend {
    s3_client: S3Client,
    bucket: String,
    key_prefix: String,
    circuit_breaker: CircuitBreaker,
}

const S3_ERROR_PRECONDITION_FAILED: &str = "PreconditionFailed";
const S3_ERROR_CONDITIONAL_REQUEST_CONFLICT: &str = "ConditionalRequestConflict";

fn is_conditional_write_conflict(error: &(impl ProvideErrorMetadata + std::fmt::Display)) -> bool {
    let code = error.code().unwrap_or_default();
    code == S3_ERROR_PRECONDITION_FAILED || code == S3_ERROR_CONDITIONAL_REQUEST_CONFLICT
}

fn classify_conditional_put_error(
    error: &(impl ProvideErrorMetadata + std::fmt::Display),
) -> Error {
    if is_conditional_write_conflict(error) {
        Error::PreconditionFailed
    } else {
        Error::Io(error.to_string())
    }
}

/// A single `list_objects_v2` page used by the pipelined `delete_prefix` loop.
struct DeletePage {
    keys: Vec<ObjectIdentifier>,
    next_token: Option<String>,
}

async fn list_page(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    continuation_token: Option<String>,
) -> Result<DeletePage, IoError> {
    let res = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
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

    let next_token = if res.is_truncated.unwrap_or(false) {
        res.next_continuation_token
    } else {
        None
    };

    Ok(DeletePage { keys, next_token })
}

async fn delete_page(
    client: &S3Client,
    bucket: &str,
    keys: Vec<ObjectIdentifier>,
) -> Result<(), IoError> {
    if keys.is_empty() {
        return Ok(());
    }

    let delete = Delete::builder()
        .set_objects(Some(keys))
        .build()
        .map_err(|e| IoError::other(e.to_string()))?;

    let result = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await
        .map_err(|e| IoError::other(e.to_string()))?;

    if let Some(errors) = result.errors
        && !errors.is_empty()
    {
        let msg = errors
            .iter()
            .filter_map(|e| e.message())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(IoError::other(format!("batch delete errors: {msg}")));
    }

    Ok(())
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        if config.multipart_part_size < ByteSize::mib(5) {
            return Err(Error::Configuration(
                "Multipart part size must be at least 5MiB".to_string(),
            ));
        }

        if config.multipart_copy_chunk_size > ByteSize::gib(5) {
            return Err(Error::Configuration(
                "Multipart copy chunk size must be at most 5GiB".to_string(),
            ));
        }

        let credentials = Credentials::new(
            &config.access_key_id,
            &config.secret_key,
            None,
            None,
            "custom",
        );

        let timeout = TimeoutConfig::builder()
            .operation_timeout(Duration::from_secs(config.operation_timeout_secs))
            .operation_attempt_timeout(Duration::from_secs(config.operation_attempt_timeout_secs))
            .build();

        let retry = RetryConfig::standard().with_max_attempts(config.max_attempts);

        let client_config = S3Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .endpoint_url(&config.endpoint)
            .credentials_provider(credentials)
            .timeout_config(timeout)
            .retry_config(retry)
            .force_path_style(true)
            .build();

        let s3_client = S3Client::from_conf(client_config);

        Ok(Self {
            s3_client,
            bucket: config.bucket.clone(),
            key_prefix: config.key_prefix.clone(),
            circuit_breaker: CircuitBreaker::new(),
        })
    }

    fn check_circuit_breaker(&self) -> Result<(), IoError> {
        self.circuit_breaker.check()
    }

    fn record_io_result<T>(&self, result: &Result<T, IoError>) {
        match result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                self.circuit_breaker.record_success();
            }
            Err(_) => self.circuit_breaker.record_failure(),
        }
    }

    fn record_data_result<T>(&self, result: &Result<T, Error>) {
        match result {
            Ok(_) | Err(Error::PreconditionFailed | Error::NotFound(_)) => {
                self.circuit_breaker.record_success();
            }
            Err(_) => self.circuit_breaker.record_failure(),
        }
    }

    fn full_key(&self, path: &str) -> String {
        if self.key_prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.key_prefix, path)
        }
    }

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
        let (body, etag, last_modified) = self.read_bytes_with_metadata(path).await?;
        Ok((body.to_vec(), etag, last_modified))
    }

    pub async fn read_bytes_with_metadata(
        &self,
        path: &str,
    ) -> Result<(Bytes, Option<String>, Option<DateTime<Utc>>), IoError> {
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

        Ok((body.into_bytes(), etag, last_modified))
    }

    pub async fn read_bytes_with_etag(
        &self,
        path: &str,
    ) -> Result<(Bytes, Option<String>), IoError> {
        let (body, etag, _) = self.read_bytes_with_metadata(path).await?;
        Ok((body, etag))
    }

    pub async fn read_bytes(&self, path: &str) -> Result<Bytes, IoError> {
        let (body, _, _) = self.read_bytes_with_metadata(path).await?;
        Ok(body)
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

        // One-deep lookahead: we always keep at most two pages in flight —
        // the current page being deleted, and the next page being listed.
        // That caps memory at ~2 × 1000 `ObjectIdentifier`s and halves the
        // serial round-trips on multi-page deletes. The per-page futures are
        // `Box::pin`ned so the outer future doesn't grow with every call
        // site (they currently carry ~10 KB of S3 SDK state each).
        let mut current = list_page(&self.s3_client, &self.bucket, &full_prefix, None).await?;

        loop {
            let DeletePage { keys, next_token } = current;

            let delete_fut = Box::pin(delete_page(&self.s3_client, &self.bucket, keys));
            let next_page = if let Some(token) = next_token {
                let next_fut = Box::pin(list_page(
                    &self.s3_client,
                    &self.bucket,
                    &full_prefix,
                    Some(token),
                ));
                let (delete_res, next_res) = tokio::join!(delete_fut, next_fut);
                delete_res?;
                Some(next_res?)
            } else {
                delete_fut.await?;
                None
            };

            match next_page {
                Some(page) => current = page,
                None => break,
            }
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

        Ok(result
            .content_length
            .unwrap_or_default()
            .try_into()
            .unwrap_or(0))
    }

    pub async fn list_prefixes(
        &self,
        path: &str,
        delimiter: &str,
        max_keys: i32,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Vec<String>, Option<String>), IoError> {
        self.check_circuit_breaker()?;
        let mut full_prefix = self.full_key(path);
        // Ensure prefix ends with / if not empty to list items inside the directory
        if !full_prefix.is_empty() && !full_prefix.ends_with('/') {
            full_prefix.push('/');
        }

        let full_start_after = start_after.map(|s| format!("{full_prefix}{s}{delimiter}"));

        let result = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .delimiter(delimiter)
            .max_keys(max_keys)
            .set_continuation_token(continuation_token)
            .set_start_after(full_start_after)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()));
        self.record_io_result(&result);
        let res = result?;

        let mut prefixes = Vec::new();
        for prefix in res.common_prefixes.unwrap_or_default() {
            if let Some(p) = prefix.prefix
                && let Some(name) = p.strip_prefix(&full_prefix)
            {
                let name = name
                    .strip_suffix(delimiter)
                    .unwrap_or(name)
                    .trim_start_matches('/');
                prefixes.push(name.to_string());
            }
        }

        let mut objects = Vec::new();
        for object in res.contents.unwrap_or_default() {
            if let Some(key) = object.key
                && let Some(name) = key.strip_prefix(&full_prefix)
            {
                objects.push(name.to_string());
            }
        }

        let next_token = if res.is_truncated.unwrap_or(false) {
            res.next_continuation_token
        } else {
            None
        };

        Ok((prefixes, objects, next_token))
    }

    pub async fn list_objects(
        &self,
        path: &str,
        max_keys: i32,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), IoError> {
        let mut full_prefix = self.full_key(path);
        // Ensure prefix ends with / if not empty to list items inside the directory
        if !full_prefix.is_empty() && !full_prefix.ends_with('/') {
            full_prefix.push('/');
        }

        let full_start_after = start_after.map(|s| format!("{full_prefix}{s}"));

        let res = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .max_keys(max_keys)
            .set_continuation_token(continuation_token)
            .set_start_after(full_start_after)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        let mut objects = Vec::new();
        for object in res.contents.unwrap_or_default() {
            if let Some(key) = object.key {
                let relative = if let Some(stripped) = key.strip_prefix(&full_prefix) {
                    stripped.trim_start_matches('/')
                } else {
                    &key
                };
                objects.push(relative.to_string());
            }
        }

        let next_token = if res.is_truncated.unwrap_or(false) {
            res.next_continuation_token
        } else {
            None
        };

        Ok((objects, next_token))
    }

    pub async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<GetObjectOutput, IoError> {
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
        result
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

    pub async fn upload_part_streaming(
        &self,
        path: &str,
        upload_id: &str,
        part_number: i32,
        content_length: u64,
        body: ByteStream,
    ) -> Result<String, IoError> {
        let key = self.full_key(path);
        let content_length =
            i64::try_from(content_length).map_err(|e| IoError::other(e.to_string()))?;

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
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok(res.e_tag.unwrap_or_default())
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

    pub async fn create_multipart_upload(&self, path: &str) -> Result<String, IoError> {
        let key = self.full_key(path);

        let res = self
            .s3_client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        res.upload_id
            .ok_or_else(|| IoError::other("upload_id not found in response"))
    }

    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<String, IoError> {
        let key = self.full_key(path);

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
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok(res.e_tag.unwrap_or_default())
    }

    pub async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        upload_id: &str,
        part_number: i32,
        range: Option<String>,
    ) -> Result<String, IoError> {
        let source_key = self.full_key(source);
        let destination_key = self.full_key(destination);

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
            .map_err(|e| IoError::other(e.to_string()))?;

        response
            .copy_part_result
            .and_then(|r| r.e_tag)
            .ok_or_else(|| IoError::other("e_tag not found in copy result"))
    }

    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<(), IoError> {
        let key = self.full_key(path);

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.s3_client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok(())
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
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok(())
    }

    pub async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<
        (
            Vec<(String, String, DateTime<Utc>)>,
            Option<String>,
            Option<String>,
        ),
        IoError,
    > {
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
            .map_err(|e| IoError::other(e.to_string()))?;

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
                uploads.push((relative_key.to_string(), upload_id, initiated));
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

            for (upload_key, upload_id, _initiated) in uploads {
                if upload_key == path {
                    return Ok(Some(upload_id));
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

    pub async fn get_object_body(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<Vec<u8>, IoError> {
        let body = self.get_object_bytes(path, offset).await?;
        Ok(body.to_vec())
    }

    pub async fn get_object_bytes(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<Bytes, IoError> {
        let res = self.get_object(path, offset).await?;
        let body = res
            .body
            .collect()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;
        Ok(body.into_bytes())
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
    ) -> Result<Vec<(i32, String, i64)>, IoError> {
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
                .map_err(|e| IoError::other(e.to_string()))?;

            for part in response.parts.unwrap_or_default() {
                if let (Some(part_number), Some(e_tag), Some(size)) =
                    (part.part_number, part.e_tag, part.size)
                {
                    parts.push((part_number, e_tag, size));
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
                aws_sdk_s3::presigning::PresigningConfig::expires_in(expires_in)
                    .map_err(|e| IoError::other(e.to_string()))?,
            )
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        Ok(presigned.uri().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(overrides: impl FnOnce(&mut BackendConfig)) -> BackendConfig {
        let mut config = BackendConfig {
            access_key_id: "key".to_string(),
            secret_key: "secret".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };
        overrides(&mut config);
        config
    }

    #[test]
    fn test_default_values() {
        let config = BackendConfig::default();
        assert_eq!(config.multipart_copy_threshold, ByteSize::gb(5));
        assert_eq!(config.multipart_copy_chunk_size, ByteSize::mb(100));
        assert_eq!(config.multipart_copy_jobs, 4);
        assert_eq!(config.multipart_part_size, ByteSize::mib(50));
        assert_eq!(config.operation_timeout_secs, 900);
        assert_eq!(config.operation_attempt_timeout_secs, 300);
        assert_eq!(config.max_attempts, 3);
    }

    #[test]
    fn test_new_multipart_part_size_too_small() {
        let config = test_config(|c| c.multipart_part_size = ByteSize::mib(4));
        let result = Backend::new(&config);
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_multipart_copy_chunk_size_too_large() {
        let config = test_config(|c| c.multipart_copy_chunk_size = ByteSize::gib(6));
        let result = Backend::new(&config);
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_valid_config() {
        let config = test_config(|_| {});
        let result = Backend::new(&config);
        assert!(result.is_ok());
        let backend = result.unwrap();
        assert_eq!(backend.bucket, "test");
        assert_eq!(backend.key_prefix, "");
    }

    #[test]
    fn test_full_key_without_prefix() {
        let config = test_config(|_| {});
        let backend = Backend::new(&config).unwrap();
        assert_eq!(backend.full_key("test/file.txt"), "test/file.txt");
    }

    #[test]
    fn test_full_key_with_prefix() {
        let config = test_config(|c| c.key_prefix = "prefix".to_string());
        let backend = Backend::new(&config).unwrap();
        assert_eq!(backend.full_key("test/file.txt"), "prefix/test/file.txt");
    }

    #[tokio::test]
    async fn test_upload_part_returns_etag() {
        let config = test_config(|c| {
            c.access_key_id = "minioadmin".to_string();
            c.secret_key = "minioadmin".to_string();
            c.bucket = "test-bucket".to_string();
        });

        let backend = Backend::new(&config).unwrap();

        let result = backend
            .upload_part(
                "test/file.txt",
                "test-upload-id",
                1,
                Bytes::from("test data"),
            )
            .await;

        if let Err(err) = result {
            assert!(err.to_string().contains("error") || err.to_string().contains("refused"));
        }
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let config = test_config(|c| {
            c.access_key_id = "minioadmin".to_string();
            c.secret_key = "minioadmin".to_string();
            c.bucket = "test-bucket".to_string();
        });

        let backend = Backend::new(&config).unwrap();

        let result = backend
            .abort_multipart_upload("test/file.txt", "test-upload-id")
            .await;

        if let Err(err) = result {
            assert!(err.to_string().contains("error") || err.to_string().contains("refused"));
        }
    }

    #[tokio::test]
    async fn test_delete_prefix_spans_multiple_list_pages() {
        // MinIO's `list_objects_v2` caps at 1000 keys per page, so seeding
        // 1100 keys forces at least two list pages. The pipelined loop must
        // handle the continuation token correctly and delete every key.
        let config = test_config(|c| {
            c.access_key_id = "root".to_string();
            c.secret_key = "roottoor".to_string();
            c.bucket = "registry".to_string();
            c.key_prefix = format!("test-delete-prefix-{}", uuid::Uuid::new_v4());
        });
        let backend = Backend::new(&config).unwrap();

        let prefix = "batch";
        let seed_count = 1100usize;
        let keys: Vec<String> = (0..seed_count)
            .map(|i| format!("{prefix}/obj-{i:05}"))
            .collect();
        let writes = keys
            .iter()
            .map(|key| backend.put_object(key, Bytes::from_static(b"x")));
        for result in futures_util::future::join_all(writes).await {
            result.unwrap();
        }

        backend.delete_prefix(prefix).await.unwrap();

        // After delete, listing returns no objects under the prefix.
        let leftover = backend
            .s3_client
            .list_objects_v2()
            .bucket(&backend.bucket)
            .prefix(backend.full_key(prefix))
            .max_keys(1)
            .send()
            .await
            .unwrap();
        assert_eq!(leftover.key_count.unwrap_or(0), 0);
    }
}
