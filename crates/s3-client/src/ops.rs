//! High-level object, multipart, listing and presign operations on top of
//! [`super::client::S3Client`].
//!
//! Every network operation runs inside [`Backend::guarded_io`] or
//! [`Backend::guarded_data`], which fail fast while the circuit breaker is
//! open and feed each outcome back into it. Only `generate_presigned_url`
//! stays outside the guard: it signs locally and never touches the wire.
//!
//! All bodies use [`bytes::Bytes`] (refcounted, zero-copy across clones and
//! retries). Reads return a streaming [`AsyncRead`] by default; the few
//! "collect-the-whole-thing" wrappers stream into a single `Bytes` allocation
//! sized from the `Content-Length` header so there is no resize churn.

use std::{io, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use reqwest::{
    Method,
    header::{HeaderMap, HeaderName},
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;

use crate::{
    Backend, Error,
    client::{
        QueryParam, S3Error, S3Response, SendOpts, content_length as parse_content_length,
        content_md5_base64, copy_source, header_string, insert_header, last_modified, range_header,
    },
    xml,
};

const MAX_MULTIPART_COPY_PARTS: u32 = 10_000;
const STREAM_BODY_PREALLOC_CAP: usize = 8 * 1024 * 1024;

/// A streaming `GetObject` result. The body pulls bytes off the HTTP socket on
/// demand (no full-payload buffering), and `content_length` reports the size
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

// guarded request helpers

impl Backend {
    /// Send `request` behind the circuit breaker and project the collected
    /// response, mapping any transport or protocol error with `map_err`. The
    /// projection runs inside the guard so a malformed success body still feeds
    /// the breaker.
    async fn send_guarded<T>(
        &self,
        request: S3Request,
        map_err: fn(&S3Error) -> io::Error,
        project: impl FnOnce(S3Response) -> Result<T, io::Error>,
    ) -> Result<T, io::Error> {
        let S3Request {
            method,
            key,
            query,
            headers,
            body,
            opts,
        } = request;
        self.guarded_io(async {
            let response = self
                .s3_client
                .send(method, &key, query, headers, body, opts)
                .await
                .map_err(|e| map_err(&e))?;
            project(response)
        })
        .await
    }

    /// Send a conditional write or delete behind the data guard, projecting the
    /// successful response and mapping a conflict to
    /// [`Error::PreconditionFailed`].
    async fn send_conditional<T>(
        &self,
        request: S3Request,
        project: impl FnOnce(S3Response) -> T,
    ) -> Result<T, Error> {
        let S3Request {
            method,
            key,
            query,
            headers,
            body,
            opts,
        } = request;
        self.guarded_data(async {
            self.s3_client
                .send(method, &key, query, headers, body, opts)
                .await
                .map(project)
                .map_err(|e| classify_conditional_error(&e))
        })
        .await
    }
}

// object-level CRUD

impl Backend {
    /// # Errors
    /// Forwards [`io::Error`] from the underlying `GET`: HTTP failures,
    /// S3 protocol errors (404 ⇒ `NotFound`), or a tripped circuit breaker.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, io::Error> {
        self.read_with_metadata(path).await.map(|(body, _, _)| body)
    }

    /// # Errors
    /// Same as [`read`](Self::read).
    pub async fn read_with_etag(&self, path: &str) -> Result<(Vec<u8>, Option<String>), io::Error> {
        self.read_with_metadata(path)
            .await
            .map(|(body, etag, _)| (body, etag))
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `GET`: HTTP failures,
    /// S3 protocol errors (404 ⇒ `NotFound`), or a tripped circuit breaker.
    pub async fn read_with_metadata(
        &self,
        path: &str,
    ) -> Result<(Vec<u8>, Option<String>, Option<DateTime<Utc>>), io::Error> {
        self.send_guarded(
            S3Request::new(Method::GET, self.full_key(path)),
            map_get_error,
            |response| {
                Ok((
                    response.body.to_vec(),
                    header_string(&response.headers, "etag"),
                    last_modified(&response.headers),
                ))
            },
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `HEAD`: HTTP failures,
    /// S3 protocol errors (404 ⇒ `NotFound`), or a tripped circuit breaker.
    pub async fn object_size(&self, path: &str) -> Result<u64, io::Error> {
        self.head_object(path).await.map(|(size, _, _)| size)
    }

    /// Single HEAD request returning size, `ETag`, and last-modified together,
    /// avoiding a redundant follow-up `GET` when the caller needs all three.
    ///
    /// # Errors
    /// Forwards [`io::Error`] from the underlying `HEAD`: HTTP failures,
    /// S3 protocol errors (404 ⇒ `NotFound`), missing/unparseable
    /// `Content-Length`, or a tripped circuit breaker.
    pub async fn head_object(
        &self,
        path: &str,
    ) -> Result<(u64, Option<String>, Option<DateTime<Utc>>), io::Error> {
        self.send_guarded(
            S3Request::new(Method::HEAD, self.full_key(path)),
            map_get_error,
            |response| {
                let size = parse_content_length(&response.headers).map_err(io::Error::other)?;
                Ok((
                    size,
                    header_string(&response.headers, "etag"),
                    last_modified(&response.headers),
                ))
            },
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying streaming `GET`: HTTP
    /// failures, S3 protocol errors (404 ⇒ `NotFound`), missing or invalid
    /// `Content-Length`, or a tripped circuit breaker.
    pub async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<GetObjectResult, io::Error> {
        let key = self.full_key(path);
        let headers = offset.map(range_header).unwrap_or_default();

        let response = self
            .guarded_io(async {
                self.s3_client
                    .send_streaming_response(Method::GET, &key, Vec::new(), headers)
                    .await
                    .map_err(|e| map_get_error(&e))
            })
            .await?;

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

    /// # Errors
    /// Same as [`get_object`](Self::get_object), plus [`io::Error`] from
    /// draining the body stream into memory.
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

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `PUT`: HTTP failures,
    /// S3 protocol errors, or a tripped circuit breaker.
    pub async fn put_object(&self, path: &str, data: impl Into<Bytes>) -> Result<(), io::Error> {
        self.send_guarded(
            S3Request {
                body: data.into(),
                ..S3Request::new(Method::PUT, self.full_key(path))
            },
            io_other,
            |_| Ok(()),
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `DELETE`: HTTP failures,
    /// S3 protocol errors, or a tripped circuit breaker. Missing objects
    /// are returned as `NotFound`.
    pub async fn delete_object(&self, path: &str) -> Result<(), io::Error> {
        self.send_guarded(
            S3Request::new(Method::DELETE, self.full_key(path)),
            io_other,
            |_| Ok(()),
        )
        .await
    }

    /// # Errors
    /// Returns [`Error::PreconditionFailed`] when an object already exists
    /// at `path`; otherwise forwards HTTP / S3 protocol errors as
    /// [`Error::Io`].
    pub async fn put_object_if_not_exists(
        &self,
        path: &str,
        data: impl Into<Bytes>,
    ) -> Result<Option<String>, Error> {
        self.conditional_put(path, "if-none-match", "*", data.into())
            .await
    }

    /// # Errors
    /// Returns [`Error::PreconditionFailed`] when the stored object's
    /// `ETag` doesn't match `etag`; otherwise forwards HTTP / S3 protocol
    /// errors as [`Error::Io`].
    pub async fn put_object_if_match(
        &self,
        path: &str,
        etag: &str,
        data: impl Into<Bytes>,
    ) -> Result<Option<String>, Error> {
        self.conditional_put(path, "if-match", etag, data.into())
            .await
    }

    /// # Errors
    /// Returns [`Error::PreconditionFailed`] when the stored object's
    /// `ETag` doesn't match `etag`; otherwise forwards HTTP / S3 protocol
    /// errors as [`Error::Io`].
    pub async fn delete_if_match(&self, path: &str, etag: &str) -> Result<(), Error> {
        let headers = single_header("if-match", etag).map_err(|e| Error::Io(e.to_string()))?;
        self.send_conditional(
            S3Request {
                headers,
                opts: SendOpts {
                    non_idempotent: true,
                    ..SendOpts::default()
                },
                ..S3Request::new(Method::DELETE, self.full_key(path))
            },
            |_| (),
        )
        .await
    }

    async fn conditional_put(
        &self,
        path: &str,
        precondition_header: &'static str,
        precondition_value: &str,
        data: Bytes,
    ) -> Result<Option<String>, Error> {
        let headers = single_header(precondition_header, precondition_value)
            .map_err(|e| Error::Io(e.to_string()))?;
        self.send_conditional(
            S3Request {
                headers,
                body: data,
                opts: SendOpts {
                    non_idempotent: true,
                    ..SendOpts::default()
                },
                ..S3Request::new(Method::PUT, self.full_key(path))
            },
            |response| header_string(&response.headers, "etag"),
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying single-shot or multipart
    /// copy: HTTP failures, S3 protocol errors (including 404 on the
    /// source), or a tripped circuit breaker.
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
        let headers = single_header(
            "x-amz-copy-source",
            &copy_source(&self.encoded_bucket, &self.full_key(source)),
        )
        .map_err(|e| io::Error::other(e.to_string()))?;
        self.send_guarded(
            S3Request {
                headers,
                opts: SendOpts {
                    check_embedded_error: true,
                    ..SendOpts::default()
                },
                ..S3Request::new(Method::PUT, self.full_key(destination))
            },
            io_other,
            |_| Ok(()),
        )
        .await
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

    /// # Errors
    /// Forwards [`io::Error`] from any of the paginated `LIST` or batch
    /// `DELETE` calls. A non-empty `<Error>` list in a batch response is
    /// joined into a single [`io::Error::other`].
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

        let parsed = self
            .send_guarded(
                S3Request {
                    query: vec![QueryParam::marker("delete")],
                    headers,
                    body,
                    ..S3Request::new(Method::POST, String::new())
                },
                io_other,
                |response| xml::parse_delete_objects(&response.body).map_err(io::Error::other),
            )
            .await?;
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

// listing

impl Backend {
    /// # Errors
    /// Forwards [`io::Error`] from the underlying `ListObjectsV2` request,
    /// XML body parse failures, or a tripped circuit breaker.
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

        self.send_guarded(
            S3Request {
                query,
                ..S3Request::new(Method::GET, String::new())
            },
            io_other,
            |response| xml::parse_list_objects_v2(&response.body).map_err(io::Error::other),
        )
        .await
    }

    /// `start_after` is a path-relative key suffix appended to the prefix
    /// verbatim; the listing starts strictly after that key. Callers wanting
    /// to skip a whole child directory append the delimiter themselves.
    ///
    /// # Errors
    /// Forwards [`io::Error`] from [`list_objects_v2_raw`](Self::list_objects_v2_raw)
    /// or a tripped circuit breaker.
    pub async fn list_prefixes(
        &self,
        path: &str,
        delimiter: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Vec<String>, Option<String>), io::Error> {
        let full_prefix = ensure_trailing_slash(self.full_key(path));
        let full_start_after = start_after.map(|s| format!("{full_prefix}{s}"));

        let res = self
            .list_objects_v2_raw(
                &full_prefix,
                max_keys,
                continuation_token,
                Some(delimiter),
                full_start_after,
            )
            .await?;

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

    /// `start_after` is a path-relative key the listing starts strictly after,
    /// appended to the prefix verbatim; it is mutually exclusive with a
    /// continuation token, so pass it only on a chain's first page.
    ///
    /// # Errors
    /// Forwards [`io::Error`] from
    /// [`list_objects_v2_raw`](Self::list_objects_v2_raw).
    pub async fn list_objects(
        &self,
        path: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), io::Error> {
        let full_prefix = ensure_trailing_slash(self.full_key(path));
        let full_start_after = start_after.map(|s| format!("{full_prefix}{s}"));
        let res = self
            .list_objects_v2_raw(
                &full_prefix,
                max_keys,
                continuation_token,
                None,
                full_start_after,
            )
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

// multipart uploads

impl Backend {
    /// # Errors
    /// Forwards [`io::Error`] from the underlying `CreateMultipartUpload`
    /// request, XML body parse failures, or a tripped circuit breaker.
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String, io::Error> {
        self.send_guarded(
            S3Request {
                query: vec![QueryParam::marker("uploads")],
                ..S3Request::new(Method::POST, self.full_key(path))
            },
            io_other,
            |response| xml::parse_create_multipart_upload(&response.body).map_err(io::Error::other),
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `UploadPart` request or a
    /// tripped circuit breaker.
    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<String, io::Error> {
        self.send_guarded(
            S3Request {
                query: part_query(upload_id, part_number),
                body,
                ..S3Request::new(Method::PUT, self.full_key(path))
            },
            io_other,
            |response| Ok(header_string(&response.headers, "etag").unwrap_or_default()),
        )
        .await
    }

    /// Streams a multipart part from a byte stream into reqwest's HTTP body.
    /// The body is signed as `UNSIGNED-PAYLOAD` (no full-payload SHA256 over
    /// chunks); use [`upload_part`](Backend::upload_part) when the payload is
    /// already in memory and signed integrity is desired.
    ///
    /// # Errors
    /// Forwards [`io::Error`] from the underlying streaming `UploadPart`
    /// request or a tripped circuit breaker.
    pub async fn upload_part_streaming<S>(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        content_length: u64,
        body: S,
    ) -> Result<String, io::Error>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        let key = self.full_key(path);
        self.guarded_io(async {
            self.s3_client
                .send_streaming_body(
                    Method::PUT,
                    &key,
                    part_query(upload_id, part_number),
                    HeaderMap::new(),
                    content_length,
                    body,
                )
                .await
                .map(|response| header_string(&response.headers, "etag").unwrap_or_default())
                .map_err(|e| io_other(&e))
        })
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `UploadPartCopy` request,
    /// XML body parse failures, or a tripped circuit breaker.
    pub async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        upload_id: &str,
        part_number: u32,
        range: Option<String>,
    ) -> Result<String, io::Error> {
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

        self.send_guarded(
            S3Request {
                query: part_query(upload_id, part_number),
                headers,
                opts: SendOpts {
                    check_embedded_error: true,
                    ..SendOpts::default()
                },
                ..S3Request::new(Method::PUT, self.full_key(destination))
            },
            io_other,
            |response| xml::parse_upload_part_copy(&response.body).map_err(io::Error::other),
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying
    /// `CompleteMultipartUpload` request, including S3-embedded `<Error>`
    /// bodies (200-with-error responses), or a tripped circuit breaker.
    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<(), io::Error> {
        let body = Bytes::from(xml::complete_multipart_upload_xml(parts));
        self.send_guarded(
            S3Request {
                query: vec![QueryParam::new("uploadId", upload_id)],
                body,
                opts: SendOpts {
                    check_embedded_error: true,
                    non_idempotent: true,
                },
                ..S3Request::new(Method::POST, self.full_key(path))
            },
            io_other,
            |_| Ok(()),
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `AbortMultipartUpload`
    /// request or a tripped circuit breaker.
    pub async fn abort_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<(), io::Error> {
        self.send_guarded(
            S3Request {
                query: vec![QueryParam::new("uploadId", upload_id)],
                ..S3Request::new(Method::DELETE, self.full_key(path))
            },
            io_other,
            |_| Ok(()),
        )
        .await
    }

    /// # Errors
    /// Forwards [`io::Error`] from the underlying `ListMultipartUploads`
    /// request, XML body parse failures, or a tripped circuit breaker.
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

        let parsed = self
            .send_guarded(
                S3Request {
                    query,
                    ..S3Request::new(Method::GET, String::new())
                },
                io_other,
                |response| {
                    xml::parse_list_multipart_uploads(&response.body).map_err(io::Error::other)
                },
            )
            .await?;

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

    /// # Errors
    /// Forwards [`io::Error`] from any of the paginated `ListParts`
    /// requests, XML body parse failures, or a tripped circuit breaker.
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
            let parsed = self
                .send_guarded(
                    S3Request {
                        query,
                        ..S3Request::new(Method::GET, key.clone())
                    },
                    io_other,
                    |response| xml::parse_list_parts(&response.body).map_err(io::Error::other),
                )
                .await?;
            parts.extend(parsed.parts);
            if parsed.is_truncated {
                part_number_marker = parsed.next_part_number_marker;
            } else {
                return Ok(parts);
            }
        }
    }
}

// presigned URLs

impl Backend {
    #[allow(
        clippy::unused_async,
        reason = "async signature matches the rest of the Backend public surface"
    )]
    /// # Errors
    /// Returns [`io::Error::other`] when `SigV4` signing fails (e.g.
    /// invalid credential characters); no network call is made.
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

// helpers

/// Parameters for a single guarded S3 request whose body is replayable
/// (`Bytes`) and whose response is collected in full. Build a default with
/// [`S3Request::new`] and override individual fields with struct-update syntax.
struct S3Request {
    method: Method,
    key: String,
    query: Vec<QueryParam>,
    headers: HeaderMap,
    body: Bytes,
    opts: SendOpts,
}

impl S3Request {
    /// A request for `method` against `key` with no query, no extra headers,
    /// an empty body and default options.
    fn new(method: Method, key: String) -> Self {
        Self {
            method,
            key,
            query: Vec::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            opts: SendOpts::default(),
        }
    }
}

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
    use crate::{BackendConfig, client::content_md5_base64, test_util::mock_config};

    const MIB: u64 = 1024 * 1024;

    fn test_config(endpoint: String) -> BackendConfig {
        BackendConfig {
            multipart_copy_threshold: ByteSize::mib(5),
            multipart_copy_chunk_size: ByteSize::mib(5),
            multipart_copy_jobs: 2,
            ..mock_config(endpoint)
        }
    }

    /// Like [`test_config`] but with a short per-attempt timeout so a delayed
    /// mock response yields a status-less (transport) `S3Error` quickly. The
    /// generous operation-level timeout leaves room for the full retry budget.
    fn fast_retry_config(endpoint: String) -> BackendConfig {
        BackendConfig {
            operation_timeout_secs: 30,
            operation_attempt_timeout_secs: 1,
            ..test_config(endpoint)
        }
    }

    /// Backend over [`test_config`] pointed at `server`.
    fn mock_backend(server: &MockServer) -> Backend {
        Backend::new(&test_config(server.uri())).unwrap()
    }

    /// Backend over [`fast_retry_config`] pointed at `server`.
    fn fast_retry_backend(server: &MockServer) -> Backend {
        Backend::new(&fast_retry_config(server.uri())).unwrap()
    }

    /// Asserts `server` received exactly `expected` requests.
    async fn assert_attempts(server: &MockServer, expected: usize, msg: &str) {
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), expected, "{msg}");
    }

    /// Asserts `server` received more than one request, proving a retry ran.
    async fn assert_retried(server: &MockServer, msg: &str) {
        let requests = server.received_requests().await.unwrap();
        assert!(requests.len() > 1, "{msg}, got {} attempts", requests.len());
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

        let backend = mock_backend(&server);
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

        let backend = mock_backend(&server);
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

        let backend = mock_backend(&server);
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

        let backend = mock_backend(&server);
        let body = stream::iter([
            Ok::<Bytes, io::Error>(Bytes::from_static(b"hello ")),
            Ok::<Bytes, io::Error>(Bytes::from_static(b"world")),
        ]);

        let etag = backend
            .upload_part_streaming("test/file.txt", "upload-id", 1, 11, body)
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

        let backend = mock_backend(&server);
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

    // The transport (status-less) error is simulated by a mock that delays its
    // response past the per-attempt timeout: reqwest's per-request timeout
    // produces a `reqwest::Error` whose `status()` is `None`, which maps to an
    // `S3Error` with `status: None`: the same shape as a connection reset or a
    // read timeout after the request was sent.

    #[tokio::test]
    async fn conditional_put_not_retried_on_transport_error() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/object"))
            .respond_with(ResponseTemplate::new(201).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;

        let backend = fast_retry_backend(&server);
        let error = backend
            .put_object_if_not_exists("object", Bytes::from_static(b"body"))
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Io(_)));
        assert_attempts(
            &server,
            1,
            "a non-idempotent conditional PUT must make exactly one attempt on a transport error",
        )
        .await;
    }

    #[tokio::test]
    async fn conditional_put_retried_on_retryable_status() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/object"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let backend = mock_backend(&server);
        let error = backend
            .put_object_if_not_exists("object", Bytes::from_static(b"body"))
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Io(_)));
        assert_retried(
            &server,
            "a non-idempotent conditional PUT must still retry on a retryable HTTP status",
        )
        .await;
    }

    #[tokio::test]
    async fn complete_multipart_upload_not_retried_on_transport_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/test-bucket/object"))
            .and(query_param("uploadId", "upload-id"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;

        let backend = fast_retry_backend(&server);
        let error = backend
            .complete_multipart_upload(
                "object",
                "upload-id",
                &[UploadedPart {
                    part_number: 1,
                    e_tag: r#""etag""#.to_string(),
                    size: 5,
                }],
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_attempts(
            &server,
            1,
            "CompleteMultipartUpload must make exactly one attempt on a transport error",
        )
        .await;
    }

    /// Opens the breaker by feeding it failures until `check()` rejects.
    fn open_breaker(backend: &Backend) {
        for _ in 0..100 {
            if backend.circuit_breaker.check().is_err() {
                return;
            }
            backend.circuit_breaker.record_failure();
        }
    }

    #[tokio::test]
    async fn open_breaker_rejects_multipart_and_list_ops_without_a_request() {
        let server = MockServer::start().await;
        let backend = mock_backend(&server);
        open_breaker(&backend);

        assert!(backend.create_multipart_upload("object").await.is_err());
        assert!(
            backend
                .upload_part("object", "id", 1, Bytes::from_static(b"x"))
                .await
                .is_err()
        );
        assert!(
            backend
                .complete_multipart_upload("object", "id", &[])
                .await
                .is_err()
        );
        assert!(
            backend
                .abort_multipart_upload("object", "id")
                .await
                .is_err()
        );
        assert!(backend.list_parts("object", "id").await.is_err());
        assert!(
            backend
                .list_multipart_uploads(None, None, None)
                .await
                .is_err()
        );
        assert!(
            backend
                .list_objects("prefix", 10, None, None)
                .await
                .is_err()
        );
        assert!(backend.delete_prefix("prefix").await.is_err());

        assert_attempts(
            &server,
            0,
            "an open breaker must reject multipart and list calls before any request is sent",
        )
        .await;
    }

    #[tokio::test]
    async fn repeated_upload_part_failures_open_the_breaker() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let config = BackendConfig {
            max_attempts: 1,
            ..test_config(server.uri())
        };
        let backend = Backend::new(&config).unwrap();

        for _ in 0..100 {
            if backend.circuit_breaker.check().is_err() {
                break;
            }
            let _ = backend
                .upload_part("object", "id", 1, Bytes::from_static(b"x"))
                .await;
        }

        assert!(
            backend.circuit_breaker.check().is_err(),
            "repeated UploadPart failures must open the circuit breaker"
        );
    }

    #[tokio::test]
    async fn put_object_retried_on_transport_error() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/object"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;

        let backend = fast_retry_backend(&server);
        let error = backend
            .put_object("object", Bytes::from_static(b"body"))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_retried(
            &server,
            "an idempotent PUT must still retry on a transport error",
        )
        .await;
    }
}
