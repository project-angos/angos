//! Low-level HTTP client for S3-compatible storage.
//!
//! This module contains everything that is "S3 the wire protocol": `SigV4`
//! signing, request building, retries, timeouts and transport. The rest of the
//! S3 backend never imports `reqwest`, `hmac`, `sha2` or other HTTP primitives
//! directly; it talks to [`S3Client`] through the few zero-copy primitives
//! exposed below.

use std::{
    borrow::Cow,
    fmt::{self, Debug, Display, Formatter},
    future::Future,
    io,
    pin::pin,
    sync::Arc,
    time::Duration,
};

use angos_backoff::Backoff;
use arc_swap::ArcSwapOption;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::{
    Stream, StreamExt, TryStreamExt,
    future::{Either, pending, select},
};
use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest as Md5Digest, Md5};
use reqwest::{
    Body, Client, Method, Response, StatusCode,
    header::{
        AUTHORIZATION, CONTENT_LENGTH, HOST, HeaderMap, HeaderName, HeaderValue, RANGE, USER_AGENT,
    },
};
use sha2::Sha256;
use tokio::{
    sync::watch,
    time::{sleep, timeout},
};
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

    /// Whether the precondition itself failed, which for `If-None-Match: *`
    /// means the object is already there. A 409 `ConditionalRequestConflict`
    /// is not this: S3 answers it while another conditional request is in
    /// flight and defines it as retry-worthy, so reading it as "already there"
    /// would let a copy-then-delete move delete a source it never copied.
    /// Whether the backend refused the request in a way retrying cannot
    /// change: a 4xx that is neither a missing key, a failed precondition, nor
    /// one of the retryable throttle statuses.
    pub fn is_client_refusal(&self) -> bool {
        self.status.is_some_and(|status| status.is_client_error())
            && !self.is_not_found()
            && !self.is_conditional_conflict()
            && !is_retryable_error(self)
    }

    pub fn is_conditional_conflict(&self) -> bool {
        self.status == Some(StatusCode::PRECONDITION_FAILED)
            || matches!(self.code.as_deref(), Some("PreconditionFailed"))
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

/// A streamed request's failure, and whether its body had gone out in full by
/// then: until it has, the pushing client paces the exchange, so a failure may
/// be its slowness or departure rather than the backend's.
pub struct StreamedFailure {
    pub error: S3Error,
    pub body_sent: bool,
}

/// Request body variants. `Bytes` payloads are replayable across retries
/// (cheap to clone: refcount only). `Stream` is one-shot; it disables
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
    /// blindly (conditional writes, multipart create and complete). When set,
    /// it is retried only on an error proving the server did not apply it: a
    /// throttle or a conditional conflict. A transport error, or a 500, 502 or
    /// 504 a proxy can answer after the write landed, is returned instead, so
    /// a replay never observes a false `PreconditionFailed` or opens a second
    /// multipart upload.
    pub non_idempotent: bool,
}

/// One reqwest client, with `read_timeout` only where a stalled transfer must
/// be cut short.
fn http_client(read_timeout: Option<Duration>) -> Result<Client, S3Error> {
    let mut builder = Client::builder()
        .use_rustls_tls()
        .pool_idle_timeout(Duration::from_secs(90));
    if let Some(read_timeout) = read_timeout {
        builder = builder.read_timeout(read_timeout);
    }
    builder
        .build()
        .map_err(|e| S3Error::configuration(format!("failed to create S3 HTTP client: {e}")))
}

#[derive(Clone)]
pub struct S3Client {
    http: Client,
    /// Serves a streamed request body, which the pushing client paces.
    streaming_http: Client,
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
    retry_backoff: Backoff,
    signing_key_cache: Arc<ArcSwapOption<CachedSigningKey>>,
    user_agent: String,
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
            .field("retry_backoff", &self.retry_backoff)
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
    value: Option<String>,
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
        // The read timeout resets on every read, so it bounds a stalled
        // transfer without bounding a slow one, which is what guards a streamed
        // download. It also runs while a request body is being sent, so an
        // upload paced by the pushing client gets a client without it.
        let http = http_client(Some(Duration::from_secs(
            config.operation_attempt_timeout_secs,
        )))?;
        let streaming_http = http_client(None)?;

        Ok(Self {
            http,
            streaming_http,
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
            retry_backoff: Backoff::exponential(Duration::from_millis(50), Duration::from_secs(1))
                .with_jitter(),
            signing_key_cache: Arc::new(ArcSwapOption::empty()),
            user_agent: config
                .user_agent
                .clone()
                .unwrap_or_else(|| USER_AGENT_VALUE.to_string()),
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
                    Some(self.operation_attempt_timeout),
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

    /// Send a request whose body is produced by a `Stream`. The body is one-shot
    /// (no retries) and signed as UNSIGNED-PAYLOAD so it never has to be
    /// buffered. The pushing client paces the body, so nothing is timed until
    /// its last byte is out, as any timer would cap that client's speed; from
    /// then on S3 owes an answer within `operation_attempt_timeout`.
    ///
    /// # Errors
    /// The failure, telling whether the body had been sent in full.
    pub async fn send_streaming_body<S>(
        &self,
        method: Method,
        key: &str,
        query: Vec<QueryParam>,
        headers: HeaderMap,
        content_length: u64,
        stream: S,
    ) -> Result<S3Response, StreamedFailure>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        let (sent_tx, mut sent_rx) = watch::channel(content_length == 0);
        let mut unsent = content_length;
        let stream = stream.inspect_ok(move |chunk| {
            unsent = unsent.saturating_sub(chunk.len() as u64);
            if unsent == 0 {
                sent_tx.send_replace(true);
            }
        });
        let exchange = async {
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
                    None,
                )
                .await?;
            let status = response.status();
            if !status.is_success() {
                return Err(S3Error::from_response(response).await);
            }
            collect_full_response(response).await
        };
        let answer_due = async {
            // A body that stops short ends the exchange on its own.
            let sent = sent_rx.wait_for(|sent| *sent).await.is_ok();
            if sent {
                sleep(self.operation_attempt_timeout).await;
            } else {
                pending::<()>().await;
            }
        };
        let result = match select(pin!(exchange), pin!(answer_due)).await {
            Either::Left((result, _)) => result,
            Either::Right(((), _)) => Err(S3Error::timeout(self.operation_attempt_timeout)),
        };
        result.map_err(|error| StreamedFailure {
            error,
            body_sent: *sent_rx.borrow(),
        })
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
                    None,
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
            // A `loop` (not a bounded `for`) so the final attempt returns its
            // own result and no post-loop arm is needed.
            let mut attempt = 1;
            loop {
                match request().await {
                    Ok(value) => return Ok(value),
                    // A non-idempotent request may already have landed unless
                    // the error proves it did not.
                    Err(error)
                        if attempt < attempts
                            && if non_idempotent {
                                is_unapplied_error(&error)
                            } else {
                                is_retryable_error(&error)
                            } =>
                    {
                        sleep(self.retry_backoff.delay(attempt - 1)).await;
                        attempt += 1;
                    }
                    Err(error) => return Err(error),
                }
            }
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
        request_timeout: Option<Duration>,
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
        insert_header(&mut headers, USER_AGENT, &self.user_agent)?;
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

        // Only a streamed request body takes the client without a read
        // timeout; a streamed download still needs one to cut a stall short.
        let client = match &body {
            RequestBody::Stream { .. } => &self.streaming_http,
            _ => &self.http,
        };
        let mut request = client.request(method, &target.url).headers(headers);
        if let Some(timeout) = request_timeout {
            request = request.timeout(timeout);
        }
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
        let string_to_sign =
            format!("{SIGNING_ALGORITHM}\n{amz_date}\n{credential_scope}\n{canonical_hash}");
        let signing_key = self.signing_key(date)?;
        Ok(hex::encode(hmac_sha256(
            &signing_key,
            string_to_sign.as_bytes(),
        )?))
    }

    fn signing_key(&self, date: &str) -> Result<[u8; 32], S3Error> {
        if let Some(cached) = self.signing_key_cache.load().as_ref()
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
        self.signing_key_cache
            .store(Some(Arc::new(CachedSigningKey {
                date: date.to_string(),
                key,
            })));
        Ok(key)
    }

    fn credential_scope(&self, date: &str) -> String {
        format!("{date}/{}/{SERVICE}/aws4_request", self.region)
    }
}

// public helpers used by the operations module

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
    // Defaulting a missing header to zero would report a stored blob as empty
    // and truncate the body served from it.
    header_string(headers, "content-length")
        .ok_or_else(|| S3Error::configuration("S3 response carries no content-length"))?
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

// internal helpers

fn build_canonical_request(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    canonical_headers: &str,
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )
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

    let canonical = values
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<Vec<_>>()
        .concat();
    let signed = values
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    Ok((canonical, signed))
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn encode_query(query: &[QueryParam]) -> Vec<EncodedQueryParam> {
    query
        .iter()
        .map(|param| EncodedQueryParam {
            name: aws_uri_encode(&param.name, true),
            value: param.value.as_deref().map(|v| aws_uri_encode(v, true)),
        })
        .collect()
}

fn canonical_query(query: &[EncodedQueryParam]) -> String {
    let mut params = query.iter().collect::<Vec<_>>();
    params.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));
    params
        .iter()
        .map(|param| {
            format!(
                "{}={}",
                param.name,
                param.value.as_deref().unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn url_query(query: &[EncodedQueryParam]) -> String {
    query
        .iter()
        .map(|param| match &param.value {
            Some(value) => format!("{}={value}", param.name),
            None => param.name.clone(),
        })
        .collect::<Vec<_>>()
        .join("&")
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

/// Whether `error` proves the server did not apply the request, the only
/// errors a non-idempotent request is replayed on: a throttle refuses before
/// acting, and S3 answers a conditional conflict instead of applying the
/// request, defining the remedy as retrying. A 500, 502 or 504 proves nothing,
/// since a proxy can send one after the write landed.
fn is_unapplied_error(error: &S3Error) -> bool {
    matches!(
        error.status,
        Some(StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE)
    ) || matches!(
        error.code.as_deref(),
        Some(
            "ConditionalRequestConflict"
                | "SlowDown"
                | "Throttling"
                | "ThrottlingException"
                | "TooManyRequestsException"
        )
    )
}

/// Whether `error` is worth replaying a request that is safe to repeat: one
/// proving nothing landed, a server or gateway failure, a transport error
/// with no status, or a timeout.
fn is_retryable_error(error: &S3Error) -> bool {
    is_unapplied_error(error)
        || error.status.is_none_or(|status| {
            matches!(
                status,
                StatusCode::INTERNAL_SERVER_ERROR
                    | StatusCode::BAD_GATEWAY
                    | StatusCode::GATEWAY_TIMEOUT
            )
        })
        || matches!(
            error.code.as_deref(),
            Some("InternalError" | "RequestTimeout" | "RequestTimeoutException")
        )
}

/// Ceiling on the pre-allocation a response's `Content-Length` asks for. The
/// header is the endpoint's word, and one large enough to exceed what the
/// allocator can serve aborts the process instead of failing the request;
/// past this the buffer grows with the bytes that actually arrive.
const MAX_BODY_PREALLOC: usize = 1024 * 1024;

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
    let mut body = BytesMut::with_capacity(hint.min(MAX_BODY_PREALLOC));
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

    /// A response with no `Content-Length` must not read as an empty object:
    /// a HEAD would report a stored blob as zero bytes and a GET would serve
    /// it truncated.
    #[test]
    fn a_missing_content_length_is_an_error_not_a_zero_size() {
        let error = content_length(&HeaderMap::new())
            .expect_err("a response carrying no content-length must not read as empty");

        assert!(
            error.to_string().contains("no content-length"),
            "got: {error}"
        );
    }

    /// The `Content-Length` a response declares is the endpoint's word, and
    /// pre-allocating it lets a broken or hostile one abort the process, since
    /// a failed allocation is not an error a caller can catch.
    #[tokio::test]
    async fn a_response_body_is_not_preallocated_from_its_declared_length() {
        let declared = http::Response::builder()
            .header(CONTENT_LENGTH, usize::MAX.to_string())
            .body("short")
            .unwrap();

        let collected = collect_full_response(Response::from(declared))
            .await
            .expect("the body that actually arrived");

        assert_eq!(collected.body, "short");
    }

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
    fn user_agent_uses_the_configured_value() {
        let client = S3Client::new(&BackendConfig {
            endpoint: "https://s3.amazonaws.com".to_string(),
            bucket: "b".to_string(),
            user_agent: Some("angos/9.9.9".to_string()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(client.user_agent, "angos/9.9.9");
    }

    #[test]
    fn user_agent_falls_back_to_this_crate_version_when_unset() {
        assert_eq!(test_client().user_agent, USER_AGENT_VALUE);
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
