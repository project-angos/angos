//! High-level object, multipart, listing and presign operations on top of
//! [`super::client::S3Client`].
//!
//! Every network operation runs inside [`Backend::guarded`], which fails fast
//! while the circuit breaker is open and feeds each outcome back into it. Only
//! `generate_presigned_url` stays outside the guard: it signs locally and never
//! touches the wire.
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
use tokio::io::AsyncRead;
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

/// A streaming `GetObject` result. The body pulls bytes off the HTTP socket on
/// demand (no full-payload buffering), and `content_length` reports the size
/// of the (possibly ranged) response.
pub struct GetObjectResult {
    pub body: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub content_length: u64,
}

/// A single completed part of a multipart upload, named as `ListParts` lists it.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
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
    /// response. Transport and protocol errors are classified by
    /// [`classify_error`] (404 to `NotFound`, a conditional 412 to
    /// `PreconditionFailed`, anything else to `Io`). The projection runs inside
    /// the guard so a malformed success body still feeds the breaker.
    async fn send_guarded<T>(
        &self,
        request: S3Request,
        project: impl FnOnce(S3Response) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let S3Request {
            method,
            key,
            query,
            headers,
            body,
            opts,
        } = request;
        self.guarded(async {
            let response = self
                .s3_client
                .send(method, &key, query, headers, body, opts)
                .await
                .map_err(|e| classify_error(&e))?;
            project(response)
        })
        .await
    }
}

// object-level CRUD

impl Backend {
    /// # Errors
    /// Forwards [`Error`] from the underlying `GET`: HTTP failures,
    /// S3 protocol errors (404 ⇒ `NotFound`), or a tripped circuit breaker.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, Error> {
        self.send_guarded(
            S3Request::new(Method::GET, self.full_key(path)),
            |response| Ok(response.body.to_vec()),
        )
        .await
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying `HEAD`: HTTP failures,
    /// S3 protocol errors (404 ⇒ `NotFound`), or a tripped circuit breaker.
    pub async fn object_size(&self, path: &str) -> Result<u64, Error> {
        self.head_object(path).await.map(|(size, _, _)| size)
    }

    /// Single HEAD request returning size, `ETag`, and last-modified together,
    /// avoiding a redundant follow-up `GET` when the caller needs all three.
    ///
    /// # Errors
    /// Forwards [`Error`] from the underlying `HEAD`: HTTP failures,
    /// S3 protocol errors (404 ⇒ `NotFound`), missing/unparseable
    /// `Content-Length`, or a tripped circuit breaker.
    pub async fn head_object(
        &self,
        path: &str,
    ) -> Result<(u64, Option<String>, Option<DateTime<Utc>>), Error> {
        self.send_guarded(
            S3Request::new(Method::HEAD, self.full_key(path)),
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
    /// Forwards [`Error`] from the underlying streaming `GET`: HTTP
    /// failures, S3 protocol errors (404 ⇒ `NotFound`), missing or invalid
    /// `Content-Length`, or a tripped circuit breaker.
    pub async fn get_object(
        &self,
        path: &str,
        offset: Option<u64>,
    ) -> Result<GetObjectResult, Error> {
        let key = self.full_key(path);
        let headers = offset.map(range_header).unwrap_or_default();

        let response = self
            .guarded(async {
                self.s3_client
                    .send_streaming_response(Method::GET, &key, Vec::new(), headers)
                    .await
                    .map_err(|e| classify_error(&e))
            })
            .await?;

        let content_length =
            parse_content_length(response.headers()).map_err(|e| Error::Io(e.to_string()))?;
        let reader = StreamReader::new(response.bytes_stream().map_err(io::Error::other));

        Ok(GetObjectResult {
            body: Box::new(reader),
            content_length,
        })
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying `PUT`: HTTP failures,
    /// S3 protocol errors, or a tripped circuit breaker.
    pub async fn put_object(&self, path: &str, data: impl Into<Bytes>) -> Result<(), Error> {
        self.send_guarded(
            S3Request {
                body: data.into(),
                ..S3Request::new(Method::PUT, self.full_key(path))
            },
            |_| Ok(()),
        )
        .await
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying `DELETE`: HTTP failures,
    /// S3 protocol errors, or a tripped circuit breaker. Missing objects
    /// are returned as `NotFound`.
    pub async fn delete_object(&self, path: &str) -> Result<(), Error> {
        self.send_guarded(S3Request::new(Method::DELETE, self.full_key(path)), |_| {
            Ok(())
        })
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

    async fn conditional_put(
        &self,
        path: &str,
        precondition_header: &'static str,
        precondition_value: &str,
        data: Bytes,
    ) -> Result<Option<String>, Error> {
        let headers = single_header(precondition_header, precondition_value)
            .map_err(|e| Error::Io(e.to_string()))?;
        self.send_guarded(
            S3Request {
                headers,
                body: data,
                opts: SendOpts {
                    non_idempotent: true,
                    ..SendOpts::default()
                },
                ..S3Request::new(Method::PUT, self.full_key(path))
            },
            |response| Ok(header_string(&response.headers, "etag")),
        )
        .await
    }

    /// Copies `source` to `destination`, returning the source's size.
    ///
    /// # Errors
    /// Forwards [`Error`] from the underlying single-shot or multipart
    /// copy: HTTP failures, S3 protocol errors (including 404 on the
    /// source), or a tripped circuit breaker.
    pub async fn copy_object(&self, source: &str, destination: &str) -> Result<u64, Error> {
        let source_size = self.object_size(source).await?;
        if source_size <= self.multipart_copy_threshold {
            self.copy_object_single(source, destination).await?;
        } else {
            self.copy_object_multipart(source, destination, source_size)
                .await?;
        }
        Ok(source_size)
    }

    async fn copy_object_single(&self, source: &str, destination: &str) -> Result<(), Error> {
        let headers = single_header(
            "x-amz-copy-source",
            &copy_source(&self.encoded_bucket, &self.full_key(source)),
        )
        .map_err(|e| Error::Io(e.to_string()))?;
        self.send_guarded(
            S3Request {
                headers,
                opts: SendOpts {
                    check_embedded_error: true,
                    ..SendOpts::default()
                },
                ..S3Request::new(Method::PUT, self.full_key(destination))
            },
            |_| Ok(()),
        )
        .await
    }

    async fn copy_object_multipart(
        &self,
        source: &str,
        destination: &str,
        source_size: u64,
    ) -> Result<(), Error> {
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
    ) -> Result<Vec<UploadedPart>, Error> {
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
                Ok::<_, Error>(UploadedPart {
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
    /// Forwards [`Error`] from any of the paginated `LIST` or batch
    /// `DELETE` calls. A non-empty `<Error>` list in a batch response is
    /// joined into a single [`Error::Io`].
    pub async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        let full_prefix = self.full_key(prefix);
        let mut continuation_token = None;
        loop {
            let res = self
                .list_objects_v2_raw(&full_prefix, 1000, continuation_token, None, None)
                .await?;
            self.delete_batch(res.contents).await?;
            if res.next_continuation_token.is_none() {
                break Ok(());
            }
            continuation_token = res.next_continuation_token;
        }
    }

    async fn delete_batch(&self, objects: Vec<String>) -> Result<(), Error> {
        if objects.is_empty() {
            return Ok(());
        }
        let body = Bytes::from(xml::delete_objects_xml(&objects));
        let headers = single_header("content-md5", &content_md5_base64(&body))
            .map_err(|e| Error::Io(e.to_string()))?;

        let parsed = self
            .send_guarded(
                S3Request {
                    query: vec![QueryParam::marker("delete")],
                    headers,
                    body,
                    ..S3Request::new(Method::POST, String::new())
                },
                |response| xml::parse_delete_objects(&response.body).map_err(Error::Io),
            )
            .await?;
        // Every entry is a failed delete, message or not: dropping the ones
        // without would tell `delete_prefix` the prefix is emptied.
        let errors: Vec<String> = parsed
            .errors
            .into_iter()
            .map(|e| e.message.unwrap_or_else(|| "unspecified".to_string()))
            .collect();
        if !errors.is_empty() {
            return Err(Error::Io(format!(
                "batch delete errors: {}",
                errors.join("; ")
            )));
        }
        Ok(())
    }
}

// listing

impl Backend {
    /// # Errors
    /// Forwards [`Error`] from the underlying `ListObjectsV2` request,
    /// XML body parse failures, or a tripped circuit breaker.
    pub async fn list_objects_v2_raw(
        &self,
        full_prefix: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        delimiter: Option<&str>,
        start_after: Option<String>,
    ) -> Result<xml::ListObjectsV2Output, Error> {
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
            |response| xml::parse_list_objects_v2(&response.body).map_err(Error::Io),
        )
        .await
    }

    /// `start_after` is a path-relative key suffix appended to the prefix
    /// verbatim; the listing starts strictly after that key. Callers wanting
    /// to skip a whole child directory append the delimiter themselves.
    ///
    /// # Errors
    /// Forwards [`Error`] from [`list_objects_v2_raw`](Self::list_objects_v2_raw)
    /// or a tripped circuit breaker.
    pub async fn list_prefixes(
        &self,
        path: &str,
        delimiter: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Vec<String>, Option<String>), Error> {
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
        let next_token = res.next_continuation_token;
        Ok((prefixes, objects, next_token))
    }

    /// `start_after` is a path-relative key the listing starts strictly after,
    /// appended to the prefix verbatim; it is mutually exclusive with a
    /// continuation token, so pass it only on a chain's first page.
    ///
    /// # Errors
    /// Forwards [`Error`] from
    /// [`list_objects_v2_raw`](Self::list_objects_v2_raw).
    pub async fn list_objects(
        &self,
        path: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), Error> {
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
        let next_token = res.next_continuation_token;
        Ok((objects, next_token))
    }
}

// multipart uploads

impl Backend {
    /// # Errors
    /// Forwards [`Error`] from the underlying `CreateMultipartUpload`
    /// request, XML body parse failures, or a tripped circuit breaker.
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String, Error> {
        self.send_guarded(
            S3Request {
                query: vec![QueryParam::marker("uploads")],
                // Replaying a lost response opens a second upload for the same
                // key, and the keyless recovery then adopts whichever it finds
                // first. The client's own retry re-derives one cleanly.
                opts: SendOpts {
                    non_idempotent: true,
                    ..SendOpts::default()
                },
                ..S3Request::new(Method::POST, self.full_key(path))
            },
            |response| xml::parse_create_multipart_upload(&response.body).map_err(Error::Io),
        )
        .await
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying `UploadPart` request or a
    /// tripped circuit breaker, and reports a response carrying no `ETag`.
    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        body: Bytes,
    ) -> Result<String, Error> {
        self.send_guarded(
            S3Request {
                query: part_query(upload_id, part_number),
                body,
                ..S3Request::new(Method::PUT, self.full_key(path))
            },
            |response| part_etag(&response.headers, part_number),
        )
        .await
    }

    /// Streams a multipart part from a byte stream into reqwest's HTTP body.
    /// The body is signed as `UNSIGNED-PAYLOAD` (no full-payload SHA256 over
    /// chunks); use [`upload_part`](Backend::upload_part) when the payload is
    /// already in memory and signed integrity is desired.
    ///
    /// # Errors
    /// Forwards [`Error`] from the underlying streaming `UploadPart`
    /// request or a tripped circuit breaker, and reports a response carrying
    /// no `ETag`.
    pub async fn upload_part_streaming<S>(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u32,
        content_length: u64,
        body: S,
    ) -> Result<String, Error>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        let key = self.full_key(path);
        self.guarded(async {
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
                .map_err(|e| classify_error(&e))
                .and_then(|response| part_etag(&response.headers, part_number))
        })
        .await
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying `UploadPartCopy` request,
    /// XML body parse failures, or a tripped circuit breaker.
    pub async fn upload_part_copy(
        &self,
        source: &str,
        destination: &str,
        upload_id: &str,
        part_number: u32,
        range: Option<String>,
    ) -> Result<String, Error> {
        let mut headers = single_header(
            "x-amz-copy-source",
            &copy_source(&self.encoded_bucket, &self.full_key(source)),
        )
        .map_err(|e| Error::Io(e.to_string()))?;
        if let Some(range) = range {
            insert_header(
                &mut headers,
                HeaderName::from_static("x-amz-copy-source-range"),
                &range,
            )
            .map_err(|e| Error::Io(e.to_string()))?;
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
            |response| xml::parse_upload_part_copy(&response.body).map_err(Error::Io),
        )
        .await
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying
    /// `CompleteMultipartUpload` request, including S3-embedded `<Error>`
    /// bodies (200-with-error responses), or a tripped circuit breaker.
    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<(), Error> {
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
            |_| Ok(()),
        )
        .await
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying `AbortMultipartUpload`
    /// request or a tripped circuit breaker.
    pub async fn abort_multipart_upload(&self, path: &str, upload_id: &str) -> Result<(), Error> {
        self.send_guarded(
            S3Request {
                query: vec![QueryParam::new("uploadId", upload_id)],
                ..S3Request::new(Method::DELETE, self.full_key(path))
            },
            |_| Ok(()),
        )
        .await
    }

    /// # Errors
    /// Forwards [`Error`] from the underlying `ListMultipartUploads`
    /// request, XML body parse failures, or a tripped circuit breaker.
    pub async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
    ) -> Result<(Vec<MultipartUpload>, Option<String>, Option<String>), Error> {
        // Always scoped: an unprefixed listing returns the bucket's uploads,
        // which on a shared bucket are another tenant's keys.
        let mut query = vec![
            QueryParam::marker("uploads"),
            QueryParam::new("prefix", self.full_key(prefix.unwrap_or_default())),
        ];
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
                |response| xml::parse_list_multipart_uploads(&response.body).map_err(Error::Io),
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

        Ok((
            uploads,
            parsed.next_key_marker,
            parsed.next_upload_id_marker,
        ))
    }

    /// # Errors
    /// Forwards [`Error`] from any of the paginated `ListParts`
    /// requests, XML body parse failures, or a tripped circuit breaker.
    pub async fn list_parts(
        &self,
        path: &str,
        upload_id: &str,
    ) -> Result<Vec<UploadedPart>, Error> {
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
                    |response| xml::parse_list_parts(&response.body).map_err(Error::Io),
                )
                .await?;
            parts.extend(parsed.parts);
            if parsed.next_part_number_marker.is_none() {
                return Ok(parts);
            }
            part_number_marker = parsed.next_part_number_marker;
        }
    }
}

// presigned URLs

impl Backend {
    /// # Errors
    /// Returns [`Error::Io`] when `SigV4` signing fails (e.g.
    /// invalid credential characters); no network call is made.
    pub fn generate_presigned_url(
        &self,
        path: &str,
        expires_in: Duration,
        response_content_type: Option<&str>,
    ) -> Result<String, Error> {
        let key = self.full_key(path);
        self.s3_client
            .presigned_get_url(&key, expires_in, response_content_type)
            .map_err(|e| Error::Io(e.to_string()))
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

struct CopyPartRange {
    part_number: u32,
    start: u64,
    end: u64,
}

/// The inclusive byte ranges of a multipart copy, one per part.
fn copy_part_ranges(
    size: u64,
    chunk_size: u64,
) -> Result<impl Iterator<Item = CopyPartRange>, Error> {
    if chunk_size == 0 {
        return Err(Error::Io(
            "multipart copy chunk size must be greater than 0".to_string(),
        ));
    }
    let parts = size.div_ceil(chunk_size);
    if parts > u64::from(MAX_MULTIPART_COPY_PARTS) {
        return Err(Error::Io(format!(
            "multipart copy requires more than {MAX_MULTIPART_COPY_PARTS} parts"
        )));
    }
    Ok((0..parts).map(move |index| {
        let start = index * chunk_size;
        CopyPartRange {
            part_number: u32::try_from(index + 1).unwrap_or(u32::MAX),
            start,
            end: start.saturating_add(chunk_size).min(size) - 1,
        }
    }))
}

/// Map an S3 transport or protocol error to the typed [`Error`]: a 404 to
/// `NotFound`, a conditional 412 to `PreconditionFailed`, a definitive 4xx to
/// `Rejected`, anything else to `Io`.
fn classify_error(error: &S3Error) -> Error {
    if error.is_not_found() {
        Error::NotFound(error.to_string())
    } else if error.is_conditional_conflict() {
        Error::PreconditionFailed
    } else if error.is_client_refusal() {
        Error::Rejected(error.to_string())
    } else {
        Error::Io(error.to_string())
    }
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

/// The `ETag` a part upload must report. `CompleteMultipartUpload` names every
/// part by entity tag, so a backend that omits one has failed the upload at
/// this part rather than later, where the completion carries no clue which
/// part was at fault.
fn part_etag(headers: &HeaderMap, part_number: u32) -> Result<String, Error> {
    header_string(headers, "etag")
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| Error::Io(format!("UploadPart {part_number} returned no ETag header")))
}

fn ensure_trailing_slash(mut s: String) -> String {
    if !s.is_empty() && !s.ends_with('/') {
        s.push('/');
    }
    s
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytesize::ByteSize;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::sleep,
    };
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

    /// `CompleteMultipartUpload` names every part by `ETag`, so a part upload
    /// that reports none has to fail here. Defaulting to an empty string
    /// deferred the rejection to the completion, which cannot say which part
    /// was at fault.
    #[tokio::test]
    async fn upload_part_reports_a_response_without_an_etag() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/test/file.txt"))
            .and(query_param("partNumber", "7"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let error = mock_backend(&server)
            .upload_part("test/file.txt", "upload-id", 7, Bytes::from_static(b"body"))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains('7'),
            "the error must name the offending part, got: {error}"
        );
    }

    /// An `ETag: ""` is the same defect reaching the completion, so it is
    /// rejected alongside an absent header.
    #[tokio::test]
    async fn upload_part_reports_an_empty_etag() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/test/file.txt"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", ""))
            .mount(&server)
            .await;

        let error = mock_backend(&server)
            .upload_part("test/file.txt", "upload-id", 1, Bytes::from_static(b"body"))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("ETag"),
            "an empty ETag must be reported as a missing one, got: {error}"
        );
    }

    #[tokio::test]
    async fn upload_part_streaming_reports_a_response_without_an_etag() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/test-bucket/test/file.txt"))
            .and(query_param("partNumber", "4"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let body = stream::iter([Ok::<Bytes, io::Error>(Bytes::from_static(b"hello"))]);
        let error = mock_backend(&server)
            .upload_part_streaming("test/file.txt", "upload-id", 4, 5, body)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains('4'),
            "the streaming path must name the offending part too, got: {error}"
        );
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

        assert!(matches!(error, Error::Io(_)), "got: {error:?}");
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

        assert!(matches!(error, Error::Io(_)), "got: {error:?}");
        assert_retried(
            &server,
            "an idempotent PUT must still retry on a transport error",
        )
        .await;
    }

    /// Serves one request: headers, then the body in chunks spaced under the
    /// read timeout but totalling more than the per-attempt timeout. Returns
    /// the address to point a backend at.
    async fn trickling_body_server(body: &'static [u8]) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 1024];
            let _ = socket.read(&mut request).await;
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
            socket.write_all(head.as_bytes()).await.unwrap();
            for byte in body {
                tokio::time::sleep(Duration::from_millis(300)).await;
                socket.write_all(&[*byte]).await.unwrap();
                socket.flush().await.unwrap();
            }
        });
        addr
    }

    /// A streamed download carries no whole-request deadline: the body is
    /// consumed by the caller, so bounding the request bounds the client's
    /// download speed. A slow but progressing transfer must complete.
    #[tokio::test]
    async fn a_slow_streaming_download_outlives_the_attempt_timeout() {
        let body: &[u8] = b"0123456789";
        let addr = trickling_body_server(body).await;
        let backend = Backend::new(&fast_retry_config(format!("http://{addr}"))).unwrap();

        let mut result = backend
            .get_object("slow/blob", None)
            .await
            .expect("headers");
        let mut downloaded = Vec::new();
        result
            .body
            .read_to_end(&mut downloaded)
            .await
            .expect("a body still flowing must not be severed by the per-attempt timeout");

        assert_eq!(downloaded, body);
    }

    /// Dropping the whole-request deadline must not leave a stalled download
    /// hanging: the client's read timeout resets per read, so a body that
    /// stops arriving still fails.
    #[tokio::test]
    async fn a_stalled_streaming_download_still_fails() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 1024];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n")
                .await
                .unwrap();
            socket.flush().await.unwrap();
            // Never sends the body.
            tokio::time::sleep(Duration::from_mins(1)).await;
        });

        let backend = Backend::new(&fast_retry_config(format!("http://{addr}"))).unwrap();
        let mut result = backend
            .get_object("stalled/blob", None)
            .await
            .expect("headers");
        let mut downloaded = Vec::new();
        let error = result
            .body
            .read_to_end(&mut downloaded)
            .await
            .expect_err("a stalled body must not hang forever");

        assert_eq!(error.kind(), std::io::ErrorKind::Other, "got: {error:?}");
    }

    /// The collected body is bounded by the length the response declares, so
    /// an endpoint cannot grow it past what it announced. This is what leaves
    /// the capped pre-allocation as the only thing standing between a lying
    /// `Content-Length` and the allocator.
    #[tokio::test]
    async fn a_body_longer_than_its_declared_length_is_truncated() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = vec![0u8; 1024];
                let _ = socket.read(&mut request).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n")
                    .await;
                // Twenty bytes for a declared five.
                let _ = socket.write_all(&[b'x'; 20]).await;
                let _ = socket.flush().await;
            }
        });

        let config = BackendConfig {
            max_attempts: 1,
            ..fast_retry_config(format!("http://{addr}"))
        };
        let backend = Backend::new(&config).unwrap();
        let body = backend.read("overlong/blob").await.expect("read");

        assert_eq!(
            body.len(),
            5,
            "the declared length bounds what is collected"
        );
    }

    /// A denied action is the backend answering, not the backend being down.
    /// Counting a run of them as failures opens the breaker and takes every
    /// other operation down with one misconfigured IAM policy.
    #[tokio::test]
    async fn repeated_denials_leave_the_breaker_closed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let config = BackendConfig {
            max_attempts: 1,
            ..test_config(server.uri())
        };
        let backend = Backend::new(&config).unwrap();

        for _ in 0..(config.circuit_breaker_threshold * 2) {
            let error = backend
                .read("denied")
                .await
                .expect_err("a denied read must surface");
            assert!(matches!(error, Error::Rejected(_)), "got {error:?}");
        }

        assert!(
            backend.circuit_breaker.check().is_ok(),
            "a denied action must leave the breaker closed"
        );
    }

    /// S3 answers 409 `ConditionalRequestConflict` while another conditional
    /// request is in flight and defines it as retry-worthy. Reading it as "the
    /// object is already there" makes a copy-then-delete move delete a source
    /// it never copied.
    #[tokio::test]
    async fn a_conditional_conflict_is_retried_and_is_not_a_failed_precondition() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(409))
            .mount(&server)
            .await;

        let error = fast_retry_backend(&server)
            .put_object_if_not_exists("object", Bytes::from_static(b"body"))
            .await
            .expect_err("a conflict that outlasts the retries must surface");

        assert!(
            !matches!(error, Error::PreconditionFailed),
            "a 409 does not mean the object exists, got {error:?}"
        );
        assert_retried(&server, "S3 defines a conditional conflict as retry-worthy").await;
    }

    /// Replaying a lost `CreateMultipartUpload` leaves two open uploads for one
    /// key, and the keyless recovery then picks whichever it finds, which fails
    /// the push at completion.
    #[tokio::test]
    async fn create_multipart_upload_is_not_replayed_on_a_lost_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                let mut request = vec![0u8; 1024];
                let _ = socket.read(&mut request).await;
                // Close without answering: a transport error carrying no status.
            }
        });

        let backend = Backend::new(&fast_retry_config(format!("http://{addr}"))).unwrap();
        backend
            .create_multipart_upload("object")
            .await
            .expect_err("a dropped connection must surface as an error");

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "a lost response must not open a second upload for the same key"
        );
    }

    /// A batch-delete error entry carrying no `<Message>` is still a failed
    /// delete; reporting success tells `delete_prefix` the prefix is emptied.
    #[tokio::test]
    async fn a_batch_delete_error_without_a_message_still_fails() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Contents><Key>prefix/doomed</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#,
            ))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult><Error><Key>prefix/doomed</Key><Code>InternalError</Code></Error></DeleteResult>"#,
            ))
            .mount(&server)
            .await;

        mock_backend(&server)
            .delete_prefix("prefix")
            .await
            .expect_err("an error entry without a message must fail the batch");
    }

    /// An unprefixed `ListMultipartUploads` returns the whole bucket, which on
    /// a shared bucket is another tenant's in-flight uploads.
    #[tokio::test]
    async fn list_multipart_uploads_always_scopes_to_the_instance_prefix() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param("prefix", "tenant/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>"#,
            ))
            .mount(&server)
            .await;

        let config = BackendConfig {
            key_prefix: "tenant".to_string(),
            ..test_config(server.uri())
        };
        Backend::new(&config)
            .unwrap()
            .list_multipart_uploads(None, None, None)
            .await
            .expect("the listing must carry the instance prefix");
    }

    /// A streamed part upload is paced by the pushing client, so it carries no
    /// S3-side deadline: a slow body that keeps flowing must complete rather
    /// than time out and count against the circuit breaker.
    #[tokio::test]
    async fn a_slow_streaming_upload_outlives_the_attempt_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", r#""slow-part""#))
            .mount(&server)
            .await;

        // Four frames 600 ms apart outlast the 1 s per-attempt timeout twice
        // over.
        let body = stream::iter(0..4).then(|_| async {
            sleep(Duration::from_millis(600)).await;
            Ok::<Bytes, io::Error>(Bytes::from_static(b"0123"))
        });

        let etag = fast_retry_backend(&server)
            .upload_part_streaming("slow/part", "upload-id", 1, 16, Box::pin(body))
            .await
            .expect("a body still flowing must not be cut off by an S3-side deadline");

        assert_eq!(etag, r#""slow-part""#);
    }
}
