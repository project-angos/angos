//! High-level object, multipart, listing and presign operations on top of
//! [`super::client::S3Client`].
//!
//! All methods on `Backend` go through three steps:
//!   1. `check_circuit_breaker` — fail fast when the backend is unhealthy.
//!   2. perform a signed request via the internal client.
//!   3. `record_*_result` — feed success/failure back into the circuit breaker.
//!
//! All bodies use [`bytes::Bytes`] (refcounted, zero-copy across clones and
//! retries). Reads return a streaming [`AsyncRead`] by default; the few
//! "collect-the-whole-thing" wrappers stream into a single `Bytes` allocation
//! sized from the `Content-Length` header so there is no resize churn.

use std::{io, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStreamExt, stream};
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName},
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::io::StreamReader;

use crate::{
    Backend, Error,
    client::{
        QueryParam, S3Error, SendOpts, content_length as parse_content_length, content_md5_base64,
        copy_source, header_string, insert_header, last_modified, range_header,
    },
    xml,
};

const MAX_MULTIPART_COPY_PARTS: u32 = 10_000;
const STREAM_BODY_PREALLOC_CAP: usize = 8 * 1024 * 1024;

/// A streaming `GetObject` result. The body pulls bytes off the HTTP socket on
/// demand — no full-payload buffering — and `content_length` reports the size
/// of the (possibly ranged) response.
pub struct GetObjectResult {
    pub body: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub content_length: u64,
}

/// A single completed part of a multipart upload.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct UploadedPart {
    pub part_number: u32,
    pub e_tag: String,
    pub size: u64,
}

/// Summary of one in-progress multipart upload returned by
/// [`Backend::list_multipart_uploads`].
#[derive(Debug)]
pub struct MultipartUpload {
    pub key: String,
    pub upload_id: String,
    pub initiated_at: DateTime<Utc>,
}

// ─── object-level CRUD ────────────────────────────────────────────────────

impl Backend {
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, io::Error> {
        self.read_with_metadata(path).await.map(|(body, _, _)| body)
    }

    pub async fn read_with_etag(&self, path: &str) -> Result<(Vec<u8>, Option<String>), io::Error> {
        self.read_with_metadata(path)
            .await
            .map(|(body, etag, _)| (body, etag))
    }

    pub async fn read_with_metadata(
        &self,
        path: &str,
    ) -> Result<(Vec<u8>, Option<String>, Option<DateTime<Utc>>), io::Error> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .send_empty(
                Method::GET,
                &key,
                Vec::new(),
                HeaderMap::new(),
                SendOpts::default(),
            )
            .await
            .map(|response| {
                (
                    response.body.to_vec(),
                    header_string(&response.headers, "etag"),
                    last_modified(&response.headers),
                )
            })
            .map_err(|e| map_get_error(&e));
        self.record_io_result(&result);
        result
    }

    pub async fn object_size(&self, path: &str) -> Result<u64, io::Error> {
        self.head_object(path).await.map(|(size, _, _)| size)
    }

    /// Single HEAD request returning size, `ETag`, and last-modified together
    /// — avoids a redundant follow-up `GET` when the caller needs all three.
    pub async fn head_object(
        &self,
        path: &str,
    ) -> Result<(u64, Option<String>, Option<DateTime<Utc>>), io::Error> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .send_empty(
                Method::HEAD,
                &key,
                Vec::new(),
                HeaderMap::new(),
                SendOpts::default(),
            )
            .await
            .map_err(|e| map_get_error(&e))
            .and_then(|response| {
                let size =
                    parse_content_length(&response.headers).map_err(io::Error::other)?;
                Ok((
                    size,
                    header_string(&response.headers, "etag"),
                    last_modified(&response.headers),
                ))
            });
        self.record_io_result(&result);
        result
    }

    pub async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<GetObjectResult, io::Error> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);
        let headers = offset.map(range_header).unwrap_or_default();

        let result = self
            .s3_client
            .send_streaming_response(Method::GET, &key, Vec::new(), headers)
            .await
            .map_err(|e| map_get_error(&e));
        self.record_io_result(&result);
        let response = result?;

        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("0")
            .parse::<u64>()
            .map_err(|e| io::Error::other(format!("invalid S3 content-length: {e}")))?;
        let reader = StreamReader::new(response.bytes_stream().map_err(io::Error::other));

        Ok(GetObjectResult {
            body: Box::new(reader),
            content_length,
        })
    }

    pub async fn get_object_body(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<Vec<u8>, io::Error> {
        let mut res = self.get_object(path, offset).await?;
        let capacity = usize::try_from(res.content_length)
            .unwrap_or(STREAM_BODY_PREALLOC_CAP)
            .min(STREAM_BODY_PREALLOC_CAP);
        let mut buf = Vec::with_capacity(capacity);
        res.body.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    pub async fn put_object(&self, path: &str, data: impl Into<Bytes>) -> Result<(), io::Error> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .send(
                Method::PUT,
                &key,
                Vec::new(),
                HeaderMap::new(),
                data.into(),
                SendOpts::default(),
            )
            .await
            .map(|_| ())
            .map_err(|e| io_other(&e));
        self.record_io_result(&result);
        result
    }

    pub async fn delete(&self, path: &str) -> Result<(), io::Error> {
        self.check_circuit_breaker()?;
        let key = self.full_key(path);

        let result = self
            .s3_client
            .send_empty(
                Method::DELETE,
                &key,
                Vec::new(),
                HeaderMap::new(),
                SendOpts::default(),
            )
            .await
            .map(|_| ())
            .map_err(|e| io_other(&e));
        self.record_io_result(&result);
        result
    }

    /// Alias kept for symmetry with the upload-side `delete_object` callers.
    pub async fn delete_object(&self, path: &str) -> Result<(), io::Error> {
        self.delete(path).await
    }

    pub async fn put_object_if_not_exists(
        &self,
        path: &str,
        data: impl Into<Bytes>,
    ) -> Result<Option<String>, Error> {
        self.conditional_put(path, "if-none-match", "*", data.into())
            .await
    }

    pub async fn put_object_if_match(
        &self,
        path: &str,
        etag: &str,
        data: impl Into<Bytes>,
    ) -> Result<Option<String>, Error> {
        self.conditional_put(path, "if-match", etag, data.into())
            .await
    }

    pub async fn delete_if_match(&self, path: &str, etag: &str) -> Result<(), Error> {
        self.check_circuit_breaker()
            .map_err(|e| Error::Io(e.to_string()))?;
        let key = self.full_key(path);
        let headers = single_header("if-match", etag).map_err(|e| Error::Io(e.to_string()))?;

        let result = self
            .s3_client
            .send_empty(
                Method::DELETE,
                &key,
                Vec::new(),
                headers,
                SendOpts::default(),
            )
            .await
            .map(|_| ())
            .map_err(|e| classify_conditional_error(&e));
        self.record_data_result(&result);
        result
    }

    async fn conditional_put(
        &self,
        path: &str,
        precondition_header: &'static str,
        precondition_value: &str,
        data: Bytes,
    ) -> Result<Option<String>, Error> {
        self.check_circuit_breaker()
            .map_err(|e| Error::Io(e.to_string()))?;
        let key = self.full_key(path);
        let headers = single_header(precondition_header, precondition_value)
            .map_err(|e| Error::Io(e.to_string()))?;

        let result = self
            .s3_client
            .send(
                Method::PUT,
                &key,
                Vec::new(),
                headers,
                data,
                SendOpts::default(),
            )
            .await
            .map(|response| header_string(&response.headers, "etag"))
            .map_err(|e| classify_conditional_error(&e));
        self.record_data_result(&result);
        result
    }

    pub async fn copy_object(&self, source: &str, destination: &str) -> Result<(), io::Error> {
        let source_size = self.object_size(source).await?;
        if source_size <= self.multipart_copy_threshold {
            self.copy_object_single(source, destination).await
        } else {
            self.copy_object_multipart(source, destination, source_size)
                .await
        }
    }

    async fn copy_object_single(&self, source: &str, destination: &str) -> Result<(), io::Error> {
        let destination_key = self.full_key(destination);
        let headers = single_header(
            "x-amz-copy-source",
            &copy_source(&self.encoded_bucket, &self.full_key(source)),
        )
        .map_err(|e| io::Error::other(e.to_string()))?;

        let result = self
            .s3_client
            .send_empty(
                Method::PUT,
                &destination_key,
                Vec::new(),
                headers,
                SendOpts {
                    check_embedded_error: true,
                },
            )
            .await
            .map(|_| ())
            .map_err(|e| io_other(&e));
        self.record_io_result(&result);
        result
    }

    async fn copy_object_multipart(
        &self,
        source: &str,
        destination: &str,
        source_size: u64,
    ) -> Result<(), io::Error> {
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
    ) -> Result<Vec<UploadedPart>, io::Error> {
        let ranges = copy_part_ranges(source_size, self.multipart_copy_chunk_size)?;
        let mut parts = stream::iter(ranges)
            .map(|range| async move {
                let e_tag = self
                    .upload_part_copy(
                        source,
                        destination,
                        upload_id,
                        range.part_number,
                        Some(format!("bytes={}-{}", range.start, range.end)),
                    )
                    .await?;
                Ok::<_, io::Error>(UploadedPart {
                    part_number: range.part_number,
                    e_tag,
                    size: range.end - range.start + 1,
                })
            })
            .buffer_unordered(self.multipart_copy_jobs.max(1))
            .try_collect::<Vec<_>>()
            .await?;
        parts.sort_by_key(|p| p.part_number);
        Ok(parts)
    }

    pub async fn delete_prefix(&self, prefix: &str) -> Result<(), io::Error> {
        let full_prefix = self.full_key(prefix);
        let mut continuation_token = None;
        loop {
            let res = self
                .list_objects_v2_raw(&full_prefix, 1000, continuation_token, None, None)
                .await?;
            self.delete_batch(res.contents).await?;
            if res.is_truncated {
                continuation_token = res.next_continuation_token;
            } else {
                break Ok(());
            }
        }
    }

    async fn delete_batch(&self, objects: Vec<String>) -> Result<(), io::Error> {
        if objects.is_empty() {
            return Ok(());
        }
        let body = Bytes::from(xml::delete_objects_xml(&objects));
        let headers = single_header("content-md5", &content_md5_base64(&body))
            .map_err(|e| io::Error::other(e.to_string()))?;

        let response = self
            .s3_client
            .send(
                Method::POST,
                "",
                vec![QueryParam::marker("delete")],
                headers,
                body,
                SendOpts::default(),
            )
            .await
            .map_err(|e| io_other(&e))?;

        let parsed = xml::parse_delete_objects(&response.body).map_err(io::Error::other)?;
        let errors: Vec<_> = parsed
            .errors
            .into_iter()
            .filter_map(|e| e.message)
            .collect();
        if let Some(err) = aggregate_batch_delete_errors(&errors) {
            return Err(err);
        }
        Ok(())
    }
}

// ─── listing ──────────────────────────────────────────────────────────────

impl Backend {
    pub async fn list_objects_v2_raw(
        &self,
        full_prefix: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        delimiter: Option<&str>,
        start_after: Option<String>,
    ) -> Result<xml::ListObjectsV2Output, io::Error> {
        let mut query = vec![
            QueryParam::new("list-type", "2"),
            QueryParam::new("prefix", full_prefix),
            QueryParam::new("max-keys", max_keys.to_string()),
        ];
        if let Some(d) = delimiter {
            query.push(QueryParam::new("delimiter", d));
        }
        if let Some(token) = continuation_token {
            query.push(QueryParam::new("continuation-token", token));
        }
        if let Some(start_after) = start_after {
            query.push(QueryParam::new("start-after", start_after));
        }

        let response = self
            .s3_client
            .send_empty(
                Method::GET,
                "",
                query,
                HeaderMap::new(),
                SendOpts::default(),
            )
            .await
            .map_err(|e| io_other(&e))?;
        xml::parse_list_objects_v2(&response.body).map_err(io::Error::other)
    }

    pub async fn list_prefixes(
        &self,
        path: &str,
        delimiter: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Vec<String>, Option<String>), io::Error> {
        self.check_circuit_breaker()?;
        let full_prefix = ensure_trailing_slash(self.full_key(path));
        let full_start_after = start_after.map(|s| format!("{full_prefix}{s}{delimiter}"));

        let result = self
            .list_objects_v2_raw(
                &full_prefix,
                max_keys,
                continuation_token,
                Some(delimiter),
                full_start_after,
            )
            .await;
        self.record_io_result(&result);
        let res = result?;

        let prefixes = res
            .common_prefixes
            .into_iter()
            .filter_map(|p| {
                p.strip_prefix(&full_prefix).map(|name| {
                    name.strip_suffix(delimiter)
                        .unwrap_or(name)
                        .trim_start_matches('/')
                        .to_string()
                })
            })
            .collect();
        let objects = res
            .contents
            .into_iter()
            .filter_map(|key| key.strip_prefix(&full_prefix).map(str::to_string))
            .collect();
        let next_token = res.next_continuation_token.filter(|_| res.is_truncated);
        Ok((prefixes, objects, next_token))
    }

    pub async fn list_objects(
        &self,
        path: &str,
        max_keys: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), io::Error> {
        let full_prefix = ensure_trailing_slash(self.full_key(path));
        let res = self
            .list_objects_v2_raw(&full_prefix, max_keys, continuation_token, None, None)
            .await?;
        let objects = res
            .contents
            .into_iter()
            .map(|key| {
                key.strip_prefix(&full_prefix)
                    .unwrap_or(&key)
                    .trim_start_matches('/')
                    .to_string()
            })
            .collect();
        let next_token = res.next_continuation_token.filter(|_| res.is_truncated);
        Ok((objects, next_token))
    }
}

// ─── multipart uploads ────────────────────────────────────────────────────

impl Backend {
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String, io::Error> {
        let key = self.full_key(path);
        let response = self
            .s3_client
            .send_empty(
                Method::POST,
                &key,
                vec![QueryParam::marker("uploads")],
                HeaderMap::new(),
                SendOpts::default(),
            )
            .await
            .map_err(|e| io_other(&e))?;
        xml::parse_create_multipart_upload(&response.body).map_err(io::Error::other)
    }

    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<String, io::Error> {
        let key = self.full_key(path);
        let response = self
            .s3_client
            .send(
                Method::PUT,
                &key,
                part_query(upload_id, part_number),
                HeaderMap::new(),
                body,
                SendOpts::default(),
            )
            .await
            .map_err(|e| io_other(&e))?;
        Ok(header_string(&response.headers, "etag").unwrap_or_default())
    }

    /// Streams a multipart part from an mpsc channel into reqwest's HTTP body.
    /// With the blob-store defaults, memory in flight per streaming part is
    /// bounded by `FRAME_BUFFER_CAPACITY * FRAME_SIZE` plus reqwest/hyper buffers.
    pub async fn upload_part_streaming(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        content_length: u64,
        rx: mpsc::Receiver<Bytes>,
    ) -> Result<String, io::Error> {
        let key = self.full_key(path);
        let body_stream = stream::unfold(rx, |mut rx| async {
            rx.recv()
                .await
                .map(|bytes| (Ok::<Bytes, io::Error>(bytes), rx))
        });
        let response = self
            .s3_client
            .send_streaming_body(
                Method::PUT,
                &key,
                part_query(upload_id, part_number),
                HeaderMap::new(),
                content_length,
                Box::pin(body_stream),
            )
            .await
            .map_err(|e| io_other(&e))?;
        Ok(header_string(&response.headers, "etag").unwrap_or_default())
    }

    pub async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        upload_id: &str,
        part_number: u32,
        range: Option<String>,
    ) -> Result<String, io::Error> {
        let destination_key = self.full_key(destination);
        let mut headers = single_header(
            "x-amz-copy-source",
            &copy_source(&self.encoded_bucket, &self.full_key(source)),
        )
        .map_err(|e| io::Error::other(e.to_string()))?;
        if let Some(range) = range {
            insert_header(
                &mut headers,
                HeaderName::from_static("x-amz-copy-source-range"),
                &range,
            )
            .map_err(|e| io::Error::other(e.to_string()))?;
        }

        let response = self
            .s3_client
            .send_empty(
                Method::PUT,
                &destination_key,
                part_query(upload_id, part_number),
                headers,
                SendOpts {
                    check_embedded_error: true,
                },
            )
            .await
            .map_err(|e| io_other(&e))?;
        xml::parse_upload_part_copy(&response.body).map_err(io::Error::other)
    }

    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<(), io::Error> {
        let key = self.full_key(path);
        let body = Bytes::from(xml::complete_multipart_upload_xml(parts));
        self.s3_client
            .send(
                Method::POST,
                &key,
                vec![QueryParam::new("uploadId", upload_id)],
                HeaderMap::new(),
                body,
                SendOpts {
                    check_embedded_error: true,
                },
            )
            .await
            .map(|_| ())
            .map_err(|e| io_other(&e))
    }

    pub async fn abort_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<(), io::Error> {
        let key = self.full_key(path);
        self.s3_client
            .send_empty(
                Method::DELETE,
                &key,
                vec![QueryParam::new("uploadId", upload_id)],
                HeaderMap::new(),
                SendOpts::default(),
            )
            .await
            .map(|_| ())
            .map_err(|e| io_other(&e))
    }

    pub async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<(Vec<MultipartUpload>, Option<String>, Option<String>), io::Error> {
        let mut query = vec![QueryParam::marker("uploads")];
        if let Some(prefix) = prefix {
            query.push(QueryParam::new("prefix", self.full_key(prefix)));
        }
        if let Some(m) = key_marker {
            query.push(QueryParam::new("key-marker", m));
        }
        if let Some(m) = upload_id_marker {
            query.push(QueryParam::new("upload-id-marker", m));
        }

        let response = self
            .s3_client
            .send_empty(
                Method::GET,
                "",
                query,
                HeaderMap::new(),
                SendOpts::default(),
            )
            .await
            .map_err(|e| io_other(&e))?;
        let parsed = xml::parse_list_multipart_uploads(&response.body).map_err(io::Error::other)?;

        let uploads = parsed
            .uploads
            .into_iter()
            .map(|u| {
                let relative_key = match u.key.strip_prefix(&self.key_prefix) {
                    Some(stripped) => stripped.trim_start_matches('/').to_string(),
                    None => u.key.clone(),
                };
                MultipartUpload {
                    key: relative_key,
                    upload_id: u.upload_id,
                    initiated_at: u.initiated_at,
                }
            })
            .collect();

        let (next_key, next_upload) = if parsed.is_truncated {
            (parsed.next_key_marker, parsed.next_upload_id_marker)
        } else {
            (None, None)
        };
        Ok((uploads, next_key, next_upload))
    }

    pub async fn search_multipart_upload_id(
        &self,
        path: &str,
    ) -> Result<Option<String>, io::Error> {
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
            if let Some(found) = uploads.into_iter().find(|u| u.key == path) {
                return Ok(Some(found.upload_id));
            }
            if next_key.is_none() {
                return Ok(None);
            }
            key_marker = next_key;
            upload_id_marker = next_upload_id;
        }
    }

    pub async fn abort_pending_uploads(&self, path: &str) -> Result<(), io::Error> {
        while let Some(upload_id) = self.search_multipart_upload_id(path).await? {
            self.abort_multipart_upload(path, &upload_id).await?;
        }
        Ok(())
    }

    pub async fn list_parts(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<Vec<UploadedPart>, io::Error> {
        let key = self.full_key(path);
        let mut parts = Vec::new();
        let mut part_number_marker: Option<u32> = None;

        loop {
            let mut query = vec![QueryParam::new("uploadId", upload_id)];
            if let Some(marker) = part_number_marker {
                query.push(QueryParam::new("part-number-marker", marker.to_string()));
            }
            let response = self
                .s3_client
                .send_empty(
                    Method::GET,
                    &key,
                    query,
                    HeaderMap::new(),
                    SendOpts::default(),
                )
                .await
                .map_err(|e| io_other(&e))?;
            let parsed = xml::parse_list_parts(&response.body).map_err(io::Error::other)?;
            parts.extend(parsed.parts);
            if parsed.is_truncated {
                part_number_marker = parsed.next_part_number_marker;
            } else {
                return Ok(parts);
            }
        }
    }
}

// ─── presigned URLs ───────────────────────────────────────────────────────

impl Backend {
    #[allow(
        clippy::unused_async,
        reason = "async signature matches the rest of the Backend public surface"
    )]
    pub async fn generate_presigned_url(
        &self,
        path: &str,
        expires_in: Duration,
        response_content_type: Option<&str>,
    ) -> Result<String, io::Error> {
        let key = self.full_key(path);
        self.s3_client
            .presigned_get_url(&key, expires_in, response_content_type)
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

// ─── helpers ──────────────────────────────────────────────────────────────

pub fn aggregate_batch_delete_errors(errors: &[String]) -> Option<io::Error> {
    (!errors.is_empty())
        .then(|| io::Error::other(format!("batch delete errors: {}", errors.join("; "))))
}

struct CopyPartRange {
    part_number: u32,
    start: u64,
    end: u64,
}

struct CopyPartRanges {
    size: u64,
    chunk_size: u64,
    start: u64,
    part_number: u32,
}

fn copy_part_ranges(size: u64, chunk_size: u64) -> Result<CopyPartRanges, io::Error> {
    if chunk_size == 0 {
        return Err(io::Error::other(
            "multipart copy chunk size must be greater than 0",
        ));
    }
    if size.div_ceil(chunk_size) > u64::from(MAX_MULTIPART_COPY_PARTS) {
        return Err(io::Error::other(format!(
            "multipart copy requires more than {MAX_MULTIPART_COPY_PARTS} parts"
        )));
    }
    Ok(CopyPartRanges {
        size,
        chunk_size,
        start: 0,
        part_number: 1,
    })
}

impl Iterator for CopyPartRanges {
    type Item = CopyPartRange;
    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.size {
            return None;
        }
        let end = self.start.saturating_add(self.chunk_size).min(self.size) - 1;
        let range = CopyPartRange {
            part_number: self.part_number,
            start: self.start,
            end,
        };
        self.start = end + 1;
        self.part_number += 1;
        Some(range)
    }
}

fn map_get_error(error: &S3Error) -> io::Error {
    if error.is_not_found() {
        io::Error::new(io::ErrorKind::NotFound, "object not found")
    } else {
        io::Error::other(error.to_string())
    }
}

fn classify_conditional_error(error: &S3Error) -> Error {
    if error.is_conditional_conflict() {
        Error::PreconditionFailed
    } else {
        Error::Io(error.to_string())
    }
}

fn io_other(error: &S3Error) -> io::Error {
    io::Error::other(error.to_string())
}

fn single_header(name: &'static str, value: &str) -> Result<HeaderMap, S3Error> {
    let mut headers = HeaderMap::new();
    insert_header(&mut headers, HeaderName::from_static(name), value)?;
    Ok(headers)
}

fn part_query(upload_id: &str, part_number: u32) -> Vec<QueryParam> {
    vec![
        QueryParam::new("partNumber", part_number.to_string()),
        QueryParam::new("uploadId", upload_id),
    ]
}

fn ensure_trailing_slash(mut s: String) -> String {
    if !s.is_empty() && !s.ends_with('/') {
        s.push('/');
    }
    s
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;
    use crate::{BackendConfig, client::content_md5_base64};

    const MIB: u64 = 1024 * 1024;

    fn test_config(endpoint: String) -> BackendConfig {
        BackendConfig {
            access_key_id: "key".to_string(),
            secret_key: "secret".to_string(),
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
        let ranges = copy_part_ranges(12 * MIB, 5 * MIB)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(ranges.len(), 3);
        assert_eq!((ranges[0].start, ranges[0].end), (0, 5 * MIB - 1));
        assert_eq!((ranges[1].start, ranges[1].end), (5 * MIB, 10 * MIB - 1));
        assert_eq!((ranges[2].start, ranges[2].end), (10 * MIB, 12 * MIB - 1));
    }

    #[test]
    fn copy_part_ranges_rejects_too_many_parts_before_iteration() {
        assert!(copy_part_ranges(10_001 * MIB, MIB).is_err());
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
        backend.copy_object("source", "destination").await.unwrap();

        let requests = server.received_requests().await.unwrap();
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
                .filter(|r| r.headers.get("x-amz-copy-source-range").is_some())
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn delete_batch_sends_required_content_md5() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/test-bucket"))
            .and(query_param("delete", ""))
            .respond_with(ResponseTemplate::new(200).set_body_string("<DeleteResult/>"))
            .mount(&server)
            .await;

        let backend = Backend::new(&test_config(server.uri())).unwrap();
        let objects = vec!["alpha".to_string(), "needs&escaping".to_string()];
        let expected_body = xml::delete_objects_xml(&objects);
        let expected_md5 = content_md5_base64(expected_body.as_bytes());

        backend.delete_batch(objects).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let request = &requests[0];
        assert_eq!(request.body, expected_body.as_bytes());
        assert_eq!(
            request.headers["content-md5"].to_str().unwrap(),
            expected_md5
        );
    }

    #[tokio::test]
    async fn copy_object_single_surfaces_embedded_success_error() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/source"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", MIB))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/destination"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<Error><Code>AccessDenied</Code><Message>copy denied</Message></Error>",
            ))
            .mount(&server)
            .await;

        let backend = Backend::new(&test_config(server.uri())).unwrap();
        let error = backend
            .copy_object("source", "destination")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("AccessDenied"));
    }

    #[tokio::test]
    async fn upload_part_streaming_sends_bounded_stream_body() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/test/file.txt"))
            .and(query_param("partNumber", "1"))
            .and(query_param("uploadId", "upload-id"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", r#""part-etag""#))
            .mount(&server)
            .await;

        let backend = Backend::new(&test_config(server.uri())).unwrap();
        let (tx, rx) = mpsc::channel(2);
        tx.send(Bytes::from_static(b"hello ")).await.unwrap();
        tx.send(Bytes::from_static(b"world")).await.unwrap();
        drop(tx);

        let etag = backend
            .upload_part_streaming("test/file.txt", "upload-id", 1, 11, rx)
            .await
            .unwrap();

        assert_eq!(etag, r#""part-etag""#);
        let requests = server.received_requests().await.unwrap();
        let request = &requests[0];
        assert_eq!(request.body, b"hello world");
        assert_eq!(request.headers["content-length"], "11");
        assert_eq!(request.headers["x-amz-content-sha256"], "UNSIGNED-PAYLOAD");
        assert!(request.headers.get("authorization").is_some());
    }

    #[tokio::test]
    async fn complete_multipart_upload_surfaces_embedded_success_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/test-bucket/test/file.txt"))
            .and(query_param("uploadId", "upload-id"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "<Error><Code>AccessDenied</Code><Message>complete denied</Message></Error>",
            ))
            .mount(&server)
            .await;

        let backend = Backend::new(&test_config(server.uri())).unwrap();
        let error = backend
            .complete_multipart_upload(
                "test/file.txt",
                "upload-id",
                &[UploadedPart {
                    part_number: 1,
                    e_tag: r#""etag""#.to_string(),
                    size: 5,
                }],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("AccessDenied"));
    }
}
