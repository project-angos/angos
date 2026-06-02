//! Low-level HTTP client for S3-compatible storage.
//!
//! This module contains everything that is "S3 the wire protocol": `SigV4`
//! signing, request building, retries, timeouts and transport. The rest of the
//! S3 backend never imports `reqwest`, `hmac`, `sha2` or other HTTP primitives
//! directly; it talks to [`S3Client`] through the few zero-copy primitives
//! exposed below.

use std::{
    borrow::Cow,
    fmt::{self, Debug, Display, Formatter, Write},
    future::Future,
    io,
    sync::Arc,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt};
use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest as Md5Digest, Md5};
use parking_lot::Mutex;
use reqwest::{
    Body, Client, Method, Response, StatusCode,
    header::{
        AUTHORIZATION, CONTENT_LENGTH, HOST, HeaderMap, HeaderName, HeaderValue, RANGE, USER_AGENT,
    },
};
use sha2::Sha256;
use smallvec::SmallVec;
use tokio::time::timeout;
use url::Url;

use crate::BackendConfig;

const SERVICE: &str = "s3";
const SIGNING_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const USER_AGENT_VALUE: &str = concat!("angos/", env!("CARGO_PKG_VERSION"));
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

/// A single query parameter, optionally without a value (S3 markers like `?uploads`).
#[derive(Clone, Debug)]
pub struct QueryParam {
    name: String,
    value: Option<String>,
}

impl QueryParam {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(value.into()),
        }
    }

    pub fn marker(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
        }
    }
}

/// A successful S3 response: parsed headers and a zero-copy [`Bytes`] body.
#[derive(Debug)]
pub struct S3Response {
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// An S3-level error. Used internally; the operations layer maps it onto
/// `io::Error` or the registry's `s3_client::Error` before crossing the
/// module boundary.
#[derive(Debug, Clone)]
pub struct S3Error {
    pub status: Option<StatusCode>,
    pub code: Option<String>,
    pub message: String,
}

impl S3Error {
    fn transport(error: &reqwest::Error) -> Self {
        Self {
            status: error.status(),
            code: None,
            message: error.to_string(),
        }
    }

    fn timeout(duration: Duration) -> Self {
        Self {
            status: None,
            code: Some("Timeout".to_string()),
            message: format!("S3 operation exceeded {}s timeout", duration.as_secs()),
        }
    }

    fn configuration(message: impl Into<String>) -> Self {
        Self {
            status: None,
            code: Some("Configuration".to_string()),
            message: message.into(),
        }
    }

    fn status_default_code(status: StatusCode) -> Option<&'static str> {
        match status {
            StatusCode::NOT_FOUND => Some("NoSuchKey"),
            StatusCode::PRECONDITION_FAILED => Some("PreconditionFailed"),
            StatusCode::CONFLICT => Some("ConditionalRequestConflict"),
            _ => None,
        }
    }

    async fn from_response(response: Response) -> Self {
        let status = response.status();
        let body = read_response_prefix(response, MAX_ERROR_BODY_BYTES).await;
        Self::from_error_body(Some(status), &body).unwrap_or_else(|| Self {
            status: Some(status),
            code: Self::status_default_code(status).map(str::to_string),
            message: status.canonical_reason().map_or_else(
                || format!("S3 returned HTTP status {status}"),
                str::to_string,
            ),
        })
    }

    fn from_error_body(status: Option<StatusCode>, body: &[u8]) -> Option<Self> {
        let parsed = super::xml::parse_error(body);
        if parsed.code.is_none() && parsed.message.is_none() {
            return None;
        }
        Some(Self {
            status,
            code: parsed.code.or_else(|| {
                status
                    .and_then(Self::status_default_code)
                    .map(str::to_string)
            }),
            message: parsed.message.unwrap_or_else(|| {
                status.and_then(|s| s.canonical_reason()).map_or_else(
                    || "S3 returned an embedded error response".to_string(),
                    str::to_string,
                )
            }),
        })
    }

    pub fn is_not_found(&self) -> bool {
        self.status == Some(StatusCode::NOT_FOUND)
            || matches!(self.code.as_deref(), Some("NoSuchKey" | "NotFound"))
    }

    pub fn is_conditional_conflict(&self) -> bool {
        self.status == Some(StatusCode::PRECONDITION_FAILED)
            || matches!(
                self.code.as_deref(),
                Some("PreconditionFailed" | "ConditionalRequestConflict")
            )
    }
}

impl Display for S3Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.code {
            Some(code) if !self.message.is_empty() => write!(f, "{code}: {}", self.message),
            Some(code) => f.write_str(code),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for S3Error {}

/// Request body variants. `Bytes` payloads are replayable across retries
/// (cheap to clone — refcount only). `Stream` is one-shot; it disables
/// payload signing (UNSIGNED-PAYLOAD) so the body never needs buffering.
pub enum RequestBody<S> {
    Empty,
    Bytes(Bytes),
    Stream { content_length: u64, stream: S },
}

/// Stream type alias used by streaming uploads.
pub type ByteStream = Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>;

/// Options selecting per-request behaviour. All flags default to `false`.
#[derive(Clone, Copy, Default)]
pub struct SendOpts {
    /// Some S3-compatible servers return HTTP 200 with an `<Error>` document
    /// in the body for `CopyObject` and `CompleteMultipartUpload`. When set,
    /// the response body is inspected and a successful HTTP status is
    /// converted into an [`S3Error`] if an embedded error is found.
    pub check_embedded_error: bool,
    /// Whether the request mutates state in a way that is unsafe to replay
    /// blindly (conditional writes/deletes, `CompleteMultipartUpload`). When
    /// set, the retry path does NOT retry on a status-less (transport) error
    /// — the request may already have landed on the server, so a blind replay
    /// could observe a false `PreconditionFailed`. Explicit retryable HTTP
    /// statuses (5xx / throttle) are still retried, because such a response
    /// proves the server did not apply the request.
    pub non_idempotent: bool,
}

#[derive(Clone)]
pub struct S3Client {
    http: Client,
    endpoint: String,
    endpoint_path: String,
    host: String,
    bucket: String,
    encoded_bucket: String,
    access_key_id: String,
    secret_key: String,
    region: String,
    operation_timeout: Duration,
    operation_attempt_timeout: Duration,
    max_attempts: u32,
    signing_key_cache: Arc<Mutex<Option<CachedSigningKey>>>,
}

#[derive(Clone)]
struct CachedSigningKey {
    date: String,
    key: [u8; 32],
}

impl Debug for S3Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Client")
            .field("endpoint", &self.endpoint)
            .field("endpoint_path", &self.endpoint_path)
            .field("host", &self.host)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("operation_timeout", &self.operation_timeout)
            .field("operation_attempt_timeout", &self.operation_attempt_timeout)
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

struct RequestTarget {
    url: String,
    canonical_uri: String,
    canonical_query: String,
}

struct EncodedQueryParam {
    name: String,
    value: String,
    has_value: bool,
}

impl S3Client {
    pub fn new(config: &BackendConfig) -> Result<Self, S3Error> {
        let endpoint_url = Url::parse(&config.endpoint)
            .map_err(|e| S3Error::configuration(format!("invalid S3 endpoint: {e}")))?;
        let host = endpoint_url
            .host_str()
            .ok_or_else(|| S3Error::configuration("S3 endpoint must include a host"))?;
        let host = match endpoint_url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };

        let endpoint = endpoint_url.origin().ascii_serialization();
        let endpoint_path = endpoint_url.path().trim_end_matches('/').to_string();
        let http = Client::builder()
            .use_rustls_tls()
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .map_err(|e| S3Error::configuration(format!("failed to create S3 HTTP client: {e}")))?;

        Ok(Self {
            http,
            endpoint,
            endpoint_path: if endpoint_path == "/" {
                String::new()
            } else {
                endpoint_path
            },
            host,
            bucket: config.bucket.clone(),
            encoded_bucket: encode_bucket(&config.bucket),
            access_key_id: config.access_key_id.clone(),
            secret_key: config.secret_key.clone(),
            region: config.region.clone(),
            operation_timeout: Duration::from_secs(config.operation_timeout_secs),
            operation_attempt_timeout: Duration::from_secs(config.operation_attempt_timeout_secs),
            max_attempts: config.max_attempts.max(1),
            signing_key_cache: Arc::new(Mutex::new(None)),
        })
    }

    /// Send a request with a replayable body (empty or Bytes) and collect the
    /// full response in a zero-copy `Bytes`. Retries on transient errors.
    pub async fn send(
        &self,
        method: Method,
        key: &str,
        query: Vec<QueryParam>,
        headers: HeaderMap,
        body: Bytes,
        opts: SendOpts,
    ) -> Result<S3Response, S3Error> {
        self.run_with_retries(opts.non_idempotent, || async {
            let response = self
                .dispatch(
                    method.clone(),
                    key,
                    &query,
                    headers.clone(),
                    RequestBody::<ByteStream>::Bytes(body.clone()),
                )
                .await?;
            let response = collect_full_response(response).await?;
            if opts.check_embedded_error
                && let Some(error) = S3Error::from_error_body(Some(StatusCode::OK), &response.body)
            {
                return Err(error);
            }
            Ok(response)
        })
        .await
    }

    /// Send an empty-body request and collect the response into `Bytes`.
    pub async fn send_empty(
        &self,
        method: Method,
        key: &str,
        query: Vec<QueryParam>,
        headers: HeaderMap,
        opts: SendOpts,
    ) -> Result<S3Response, S3Error> {
        self.send(method, key, query, headers, Bytes::new(), opts)
            .await
    }

    /// Send a request whose body is produced by a `Stream`. The body is one-shot
    /// (no retries) and signed as UNSIGNED-PAYLOAD so it never has to be
    /// buffered. Uses the operation-level timeout but a single attempt.
    pub async fn send_streaming_body<S>(
        &self,
        method: Method,
        key: &str,
        query: Vec<QueryParam>,
        headers: HeaderMap,
        content_length: u64,
        stream: S,
    ) -> Result<S3Response, S3Error>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        let fut = async {
            let response = self
                .dispatch(
                    method,
                    key,
                    &query,
                    headers,
                    RequestBody::Stream {
                        content_length,
                        stream,
                    },
                )
                .await?;
            let status = response.status();
            if !status.is_success() {
                return Err(S3Error::from_response(response).await);
            }
            collect_full_response(response).await
        };
        timeout(self.operation_timeout, fut)
            .await
            .map_err(|_| S3Error::timeout(self.operation_timeout))?
    }

    /// Send a request and return the raw response for streaming download.
    pub async fn send_streaming_response(
        &self,
        method: Method,
        key: &str,
        query: Vec<QueryParam>,
        headers: HeaderMap,
    ) -> Result<Response, S3Error> {
        self.run_with_retries(false, || async {
            let response = self
                .dispatch(
                    method.clone(),
                    key,
                    &query,
                    headers.clone(),
                    RequestBody::<ByteStream>::Empty,
                )
                .await?;
            if response.status().is_success() {
                Ok(response)
            } else {
                Err(S3Error::from_response(response).await)
            }
        })
        .await
    }

    /// Build a presigned `GET` URL valid for `expires_in`.
    pub fn presigned_get_url(
        &self,
        key: &str,
        expires_in: Duration,
        response_content_type: Option<&str>,
    ) -> Result<String, S3Error> {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let credential_scope = self.credential_scope(&date);

        let mut query = vec![
            QueryParam::new("X-Amz-Algorithm", SIGNING_ALGORITHM),
            QueryParam::new(
                "X-Amz-Credential",
                format!("{}/{credential_scope}", self.access_key_id),
            ),
            QueryParam::new("X-Amz-Date", amz_date.clone()),
            QueryParam::new("X-Amz-Expires", expires_in.as_secs().to_string()),
            QueryParam::new("X-Amz-SignedHeaders", "host"),
        ];
        if let Some(content_type) = response_content_type {
            query.push(QueryParam::new("response-content-type", content_type));
        }

        let target = self.request_target(key, &query);
        let canonical_headers = format!("host:{}\n", self.host);
        let canonical_request = build_canonical_request(
            Method::GET.as_str(),
            &target.canonical_uri,
            &target.canonical_query,
            &canonical_headers,
            "host",
            UNSIGNED_PAYLOAD,
        );
        let signature = self.signature(&date, &amz_date, &credential_scope, &canonical_request)?;

        let mut url = target.url;
        url.push(if target.canonical_query.is_empty() {
            '?'
        } else {
            '&'
        });
        url.push_str("X-Amz-Signature=");
        url.push_str(&aws_uri_encode(&signature, true));
        Ok(url)
    }

    async fn run_with_retries<R, F, Fut>(
        &self,
        non_idempotent: bool,
        mut request: F,
    ) -> Result<R, S3Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<R, S3Error>>,
    {
        let attempts = self.max_attempts;
        let fut = async {
            for attempt in 1..=attempts {
                match request().await {
                    Ok(value) => return Ok(value),
                    // A non-idempotent request is not retried on a status-less
                    // (transport) error: the outcome is ambiguous because the
                    // request may already have landed. An explicit retryable
                    // HTTP status is still retried — the server then proved it
                    // did not apply the request.
                    Err(error)
                        if is_retryable_error(&error)
                            && !(non_idempotent && error.status.is_none())
                            && attempt < attempts => {}
                    Err(error) => return Err(error),
                }
            }
            unreachable!("attempt loop always returns");
        };
        timeout(self.operation_timeout, fut)
            .await
            .map_err(|_| S3Error::timeout(self.operation_timeout))?
    }

    async fn dispatch<S>(
        &self,
        method: Method,
        key: &str,
        query: &[QueryParam],
        mut headers: HeaderMap,
        body: RequestBody<S>,
    ) -> Result<Response, S3Error>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
    {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let (payload_hash, content_length) = match &body {
            RequestBody::Empty => (Cow::Borrowed(EMPTY_PAYLOAD_SHA256), Some(0)),
            RequestBody::Bytes(b) if b.is_empty() => (Cow::Borrowed(EMPTY_PAYLOAD_SHA256), Some(0)),
            RequestBody::Bytes(b) => (Cow::Owned(hex_sha256(b.as_ref())), Some(b.len() as u64)),
            RequestBody::Stream { content_length, .. } => {
                (Cow::Borrowed(UNSIGNED_PAYLOAD), Some(*content_length))
            }
        };
        let target = self.request_target(key, query);

        insert_header(&mut headers, HOST, &self.host)?;
        insert_header(&mut headers, USER_AGENT, USER_AGENT_VALUE)?;
        insert_header(
            &mut headers,
            HeaderName::from_static("x-amz-date"),
            &amz_date,
        )?;
        insert_header(
            &mut headers,
            HeaderName::from_static("x-amz-content-sha256"),
            &payload_hash,
        )?;
        if let Some(len) = content_length {
            insert_header(&mut headers, CONTENT_LENGTH, &len.to_string())?;
        }

        let credential_scope = self.credential_scope(&date);
        let authorization = self.authorization(
            &method,
            &target,
            &headers,
            &date,
            &amz_date,
            &credential_scope,
        )?;
        insert_header(&mut headers, AUTHORIZATION, &authorization)?;

        let mut request = self
            .http
            .request(method, &target.url)
            .headers(headers)
            .timeout(self.operation_attempt_timeout);
        request = match body {
            RequestBody::Empty => request,
            RequestBody::Bytes(b) if b.is_empty() => request,
            RequestBody::Bytes(b) => request.body(Body::from(b)),
            RequestBody::Stream { stream, .. } => request.body(Body::wrap_stream(stream)),
        };

        request.send().await.map_err(|e| S3Error::transport(&e))
    }

    fn request_target(&self, key: &str, query: &[QueryParam]) -> RequestTarget {
        let encoded_key = aws_uri_encode(key, false);
        let encoded_query = encode_query(query);

        let mut canonical_uri = String::new();
        if !self.endpoint_path.is_empty() {
            canonical_uri.push_str(&self.endpoint_path);
        }
        canonical_uri.push('/');
        canonical_uri.push_str(&self.encoded_bucket);
        if !encoded_key.is_empty() {
            canonical_uri.push('/');
            canonical_uri.push_str(&encoded_key);
        }

        let canonical_query = canonical_query(&encoded_query);
        let url_query = url_query(&encoded_query);
        let mut url = format!("{}{canonical_uri}", self.endpoint);
        if !url_query.is_empty() {
            url.push('?');
            url.push_str(&url_query);
        }

        RequestTarget {
            url,
            canonical_uri,
            canonical_query,
        }
    }

    fn authorization(
        &self,
        method: &Method,
        target: &RequestTarget,
        headers: &HeaderMap,
        date: &str,
        amz_date: &str,
        credential_scope: &str,
    ) -> Result<String, S3Error> {
        let (canonical_headers, signed_headers) = canonical_headers(headers)?;
        let payload_hash = header_value(headers, "x-amz-content-sha256")?;
        let canonical_request = build_canonical_request(
            method.as_str(),
            &target.canonical_uri,
            &target.canonical_query,
            &canonical_headers,
            &signed_headers,
            &payload_hash,
        );
        let signature = self.signature(date, amz_date, credential_scope, &canonical_request)?;
        Ok(format!(
            "{SIGNING_ALGORITHM} Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key_id
        ))
    }

    fn signature(
        &self,
        date: &str,
        amz_date: &str,
        credential_scope: &str,
        canonical_request: &str,
    ) -> Result<String, S3Error> {
        let canonical_hash = hex_sha256(canonical_request.as_bytes());
        let mut string_to_sign = String::with_capacity(
            SIGNING_ALGORITHM.len()
                + amz_date.len()
                + credential_scope.len()
                + canonical_hash.len()
                + 3,
        );
        // Writing to a `String` is infallible; discard the always-`Ok` result.
        let _ = write!(
            string_to_sign,
            "{SIGNING_ALGORITHM}\n{amz_date}\n{credential_scope}\n{canonical_hash}"
        );
        let signing_key = self.signing_key(date)?;
        Ok(hex::encode(hmac_sha256(
            &signing_key,
            string_to_sign.as_bytes(),
        )?))
    }

    fn signing_key(&self, date: &str) -> Result<[u8; 32], S3Error> {
        if let Some(cached) = self.signing_key_cache.lock().as_ref()
            && cached.date == date
        {
            return Ok(cached.key);
        }
        let date_key = hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            date.as_bytes(),
        )?;
        let date_region_key = hmac_sha256(&date_key, self.region.as_bytes())?;
        let date_region_service_key = hmac_sha256(&date_region_key, SERVICE.as_bytes())?;
        let key = hmac_sha256(&date_region_service_key, b"aws4_request")?;
        *self.signing_key_cache.lock() = Some(CachedSigningKey {
            date: date.to_string(),
            key,
        });
        Ok(key)
    }

    fn credential_scope(&self, date: &str) -> String {
        format!("{date}/{}/{SERVICE}/aws4_request", self.region)
    }
}

// ─── public helpers used by the operations module ─────────────────────────

pub fn range_header(offset: u64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    // `bytes={offset}-` is always valid ASCII; skip the header on the impossible
    // parse error rather than panicking.
    if let Ok(value) = HeaderValue::from_str(&format!("bytes={offset}-")) {
        headers.insert(RANGE, value);
    }
    headers
}

pub fn insert_header(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: &str,
) -> Result<(), S3Error> {
    let value = HeaderValue::from_str(value)
        .map_err(|e| S3Error::configuration(format!("invalid S3 header value: {e}")))?;
    headers.insert(name, value);
    Ok(())
}

pub fn header_value(headers: &HeaderMap, name: &str) -> Result<String, S3Error> {
    headers
        .get(name)
        .ok_or_else(|| S3Error::configuration(format!("missing signed header: {name}")))?
        .to_str()
        .map(ToString::to_string)
        .map_err(|e| S3Error::configuration(format!("invalid header value: {e}")))
}

pub fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

pub fn content_length(headers: &HeaderMap) -> Result<u64, S3Error> {
    header_string(headers, "content-length")
        .unwrap_or_else(|| "0".to_string())
        .parse::<u64>()
        .map_err(|e| S3Error::configuration(format!("invalid S3 content-length: {e}")))
}

pub fn last_modified(headers: &HeaderMap) -> Option<DateTime<Utc>> {
    header_string(headers, "last-modified")
        .and_then(|value| DateTime::parse_from_rfc2822(&value).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn encode_bucket(bucket: &str) -> String {
    aws_uri_encode(bucket, true)
}

pub fn copy_source(encoded_bucket: &str, key: &str) -> String {
    format!("{encoded_bucket}/{}", aws_uri_encode(key, false))
}

pub fn content_md5_base64(body: &[u8]) -> String {
    BASE64_STANDARD.encode(Md5::digest(body))
}

// ─── internal helpers ─────────────────────────────────────────────────────

fn build_canonical_request(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    canonical_headers: &str,
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    let mut out = String::with_capacity(
        method.len()
            + canonical_uri.len()
            + canonical_query.len()
            + canonical_headers.len()
            + signed_headers.len()
            + payload_hash.len()
            + 5,
    );
    out.push_str(method);
    out.push('\n');
    out.push_str(canonical_uri);
    out.push('\n');
    out.push_str(canonical_query);
    out.push('\n');
    out.push_str(canonical_headers);
    out.push('\n');
    out.push_str(signed_headers);
    out.push('\n');
    out.push_str(payload_hash);
    out
}

fn canonical_headers(headers: &HeaderMap) -> Result<(String, String), S3Error> {
    let mut values = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        if name == AUTHORIZATION {
            continue;
        }
        let name = name.as_str().to_ascii_lowercase();
        let value = normalize_header_value(
            value
                .to_str()
                .map_err(|e| S3Error::configuration(format!("invalid signed header: {e}")))?,
        );
        values.push((name, value));
    }
    values.sort_by(|a, b| a.0.cmp(&b.0));

    let mut canonical = String::new();
    let mut signed = String::new();
    for (index, (name, value)) in values.into_iter().enumerate() {
        canonical.push_str(&name);
        canonical.push(':');
        canonical.push_str(&value);
        canonical.push('\n');
        if index > 0 {
            signed.push(';');
        }
        signed.push_str(&name);
    }
    Ok((canonical, signed))
}

fn normalize_header_value(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    for part in value.split_whitespace() {
        if !normalized.is_empty() {
            normalized.push(' ');
        }
        normalized.push_str(part);
    }
    normalized
}

fn encode_query(query: &[QueryParam]) -> SmallVec<[EncodedQueryParam; 8]> {
    query
        .iter()
        .map(|param| EncodedQueryParam {
            name: aws_uri_encode(&param.name, true),
            value: param
                .value
                .as_deref()
                .map(|v| aws_uri_encode(v, true))
                .unwrap_or_default(),
            has_value: param.value.is_some(),
        })
        .collect()
}

fn canonical_query(query: &[EncodedQueryParam]) -> String {
    let mut params = query.iter().collect::<SmallVec<[&EncodedQueryParam; 8]>>();
    params.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));
    let mut encoded = String::new();
    for (index, param) in params.into_iter().enumerate() {
        if index > 0 {
            encoded.push('&');
        }
        encoded.push_str(&param.name);
        encoded.push('=');
        encoded.push_str(&param.value);
    }
    encoded
}

fn url_query(query: &[EncodedQueryParam]) -> String {
    let mut encoded = String::new();
    for (index, param) in query.iter().enumerate() {
        if index > 0 {
            encoded.push('&');
        }
        encoded.push_str(&param.name);
        if param.has_value {
            encoded.push('=');
            encoded.push_str(&param.value);
        }
    }
    encoded
}

fn aws_uri_encode(input: &str, encode_slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(input.len());
    for &byte in input.as_bytes() {
        let allowed = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (!encode_slash && byte == b'/');
        if allowed {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

fn hex_sha256(input: &[u8]) -> String {
    hex::encode(Sha256::digest(input))
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Result<[u8; 32], S3Error> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|e| S3Error::configuration(format!("invalid signing key: {e}")))?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().into())
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn is_retryable_error(error: &S3Error) -> bool {
    error.status.is_none_or(is_retryable_status)
        || matches!(
            error.code.as_deref(),
            Some(
                "InternalError"
                    | "RequestTimeout"
                    | "RequestTimeoutException"
                    | "SlowDown"
                    | "Throttling"
                    | "ThrottlingException"
                    | "TooManyRequestsException"
            )
        )
}

async fn collect_full_response(response: Response) -> Result<S3Response, S3Error> {
    let status = response.status();
    if !status.is_success() {
        return Err(S3Error::from_response(response).await);
    }
    let headers = response.headers().clone();
    let hint = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = BytesMut::with_capacity(hint);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk.map_err(|e| S3Error::transport(&e))?);
    }
    Ok(S3Response {
        headers,
        body: body.freeze(),
    })
}

async fn read_response_prefix(response: Response, limit: usize) -> Bytes {
    let mut stream = response.bytes_stream();
    let mut body = BytesMut::with_capacity(limit.min(8 * 1024));

    while body.len() < limit {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let Ok(chunk) = chunk else {
            break;
        };
        let remaining = limit - body.len();
        let take = remaining.min(chunk.len());
        body.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            break;
        }
    }
    body.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> S3Client {
        S3Client::new(&BackendConfig {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
            endpoint: "https://s3.amazonaws.com".to_string(),
            bucket: "examplebucket".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn aws_uri_encoding_preserves_key_slashes() {
        assert_eq!(aws_uri_encode("a b/c+d", false), "a%20b/c%2Bd");
        assert_eq!(aws_uri_encode("a b/c+d", true), "a%20b%2Fc%2Bd");
    }

    #[test]
    fn canonical_query_sorts_and_uses_empty_marker_values() {
        let query = vec![
            QueryParam::new("uploadId", "abc/123"),
            QueryParam::marker("uploads"),
            QueryParam::new("partNumber", "1"),
        ];
        let encoded = encode_query(&query);
        assert_eq!(
            canonical_query(&encoded),
            "partNumber=1&uploadId=abc%2F123&uploads="
        );
        assert_eq!(
            url_query(&encoded),
            "uploadId=abc%2F123&uploads&partNumber=1"
        );
    }

    #[test]
    fn request_target_uses_path_style_urls() {
        let client = test_client();
        let target = client.request_target("photos/raw image.jpg", &[]);
        assert_eq!(
            target.url,
            "https://s3.amazonaws.com/examplebucket/photos/raw%20image.jpg"
        );
        assert_eq!(
            target.canonical_uri,
            "/examplebucket/photos/raw%20image.jpg"
        );
    }

    #[test]
    fn request_target_preserves_endpoint_base_path_once() {
        let client = S3Client::new(&BackendConfig {
            access_key_id: "key".to_string(),
            secret_key: "secret".to_string(),
            endpoint: "https://object.example.com/base/".to_string(),
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        })
        .unwrap();
        let target = client.request_target("k", &[]);
        assert_eq!(target.url, "https://object.example.com/base/bucket/k");
        assert_eq!(target.canonical_uri, "/base/bucket/k");
    }

    #[test]
    fn canonical_headers_are_lowercase_sorted() {
        let mut headers = HeaderMap::new();
        insert_header(
            &mut headers,
            HeaderName::from_static("x-amz-date"),
            "20260517T120000Z",
        )
        .unwrap();
        insert_header(&mut headers, HOST, "example.com").unwrap();
        let (canonical, signed) = canonical_headers(&headers).unwrap();
        assert_eq!(canonical, "host:example.com\nx-amz-date:20260517T120000Z\n");
        assert_eq!(signed, "host;x-amz-date");
    }

    #[test]
    fn presigned_url_contains_signature_fields() {
        let client = test_client();
        let url = client
            .presigned_get_url(
                "blob",
                Duration::from_mins(1),
                Some("application/octet-stream"),
            )
            .unwrap();
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Signature="));
        assert!(url.contains("response-content-type=application%2Foctet-stream"));
    }
}
