#[cfg(test)]
mod tests;

mod auth;
mod error;
mod write;

use std::{
    collections::HashSet, fmt::Display, future::Future, io, path::PathBuf, str::FromStr, sync::Arc,
    time::Duration,
};

use futures_util::TryStreamExt;
use reqwest::{
    Client, Method, RequestBuilder, Response, StatusCode,
    header::{
        ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, HeaderValue, LINK,
        RANGE,
    },
    redirect::Policy,
};
use serde::Deserialize;
use tokio::{io::AsyncReadExt, sync::Mutex};
use tokio_util::io::StreamReader;
use tracing::{debug, instrument, warn};
use url::Url;

use angos_oci::client;
use angos_oci::header::DOCKER_CONTENT_DIGEST;
use angos_oci::http_range::ResponseRange;
use angos_oci::request::{
    GetBlobRequest, GetManifestRequest, GetReferrersRequest, HeadBlobRequest, HeadManifestRequest,
    ListTagsRequest,
};
use angos_oci::response::{ManifestHeadResponse, ManifestResponse, TagsListResponse};
use angos_oci::{Content, Descriptor, Digest, Manifest, MediaRange, MediaType, Tag};

pub use crate::{error::Error, write::UploadSession};
use angos_cache::Cache;
use angos_mtls_client::MtlsClientBuilder;
use angos_secret::Secret;

use angos_storage::BoxedReader;
use auth::token_index_cache_key;

/// Header carrying the originating event timestamp (RFC 3339) of a replication
/// request; the receiver rejects with a 409 [`REPLICATION_SUPERSEDED_CODE`]
/// when its local tag is strictly newer (last-writer-wins).
pub const X_ANGOS_SOURCE_TIMESTAMP: &str = "X-Angos-Source-Timestamp";

/// OCI error `code` returned when a replication write loses last-writer-wins.
/// Shared by sender and receiver so the sender can treat this 409 as
/// convergence (job completes) while any other 409 still retries/dead-letters.
pub const REPLICATION_SUPERSEDED_CODE: &str = "REPLICATION_SUPERSEDED";

/// Classifies a non-success read status: a true 404 maps to `not_found` so
/// callers can tell a genuinely absent object from a transient probe failure,
/// a 405 maps to `Unsupported` (the remote rejects the method), and a 416
/// answers a ranged blob fetch the remote cannot satisfy.
fn classify_read_failure(status: StatusCode, op: &str, not_found: Error) -> Error {
    match status {
        StatusCode::NOT_FOUND => not_found,
        StatusCode::METHOD_NOT_ALLOWED => Error::Unsupported,
        StatusCode::RANGE_NOT_SATISFIABLE => Error::RangeNotSatisfiable,
        _ => Error::Internal(format!("{op}: downstream returned status {status}")),
    }
}

/// The location without its query string, which on an upload-session URL
/// carries signed state that must not reach the logs.
fn without_query(location: &str) -> &str {
    match location.split_once('?') {
        Some((base, _)) => base,
        None => location,
    }
}

/// A `403` is terminal: the credential is valid and simply lacks the scope, so
/// no retry can clear it. Applied to the refreshed attempt as well, which is
/// the normal scope-denied token flow.
fn denied_if_forbidden(response: &Response) -> Result<(), Error> {
    if response.status() == StatusCode::FORBIDDEN {
        return Err(Error::Denied("Access forbidden".to_string()));
    }
    Ok(())
}

/// The media type a manifest answer was served under, `None` when the remote
/// sent no `Content-Type` or one that is not a media type. Read apart from the
/// other headers because only this one may carry a parameter section.
fn response_media_type(response: &Response) -> Option<MediaType> {
    let content_type = response.headers().get(CONTENT_TYPE)?.to_str().ok()?;

    MediaType::from_content_type(content_type).ok()
}

/// Reads and parses a required response header, naming it in an `Internal` error
/// when it is absent or unparseable (a protocol fault, not an unsupported
/// operation).
fn parse_header<T: FromStr>(
    response: &Response,
    header: impl reqwest::header::AsHeaderName + Display,
) -> Result<T, Error> {
    let name = header.to_string();
    response
        .headers()
        .get(header)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Internal(format!("missing or invalid '{name}' response header")))
}

/// The mTLS client identity. Holding both files together is what makes a
/// certificate without its key unrepresentable.
#[derive(Clone, Debug, PartialEq)]
pub struct MtlsIdentity {
    pub client_certificate: PathBuf,
    pub client_private_key: PathBuf,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(try_from = "RegistryClientConfigFields")]
pub struct RegistryClientConfig {
    pub url: String,
    pub max_redirect: u8,
    /// Bounds establishing the connection (TCP + TLS handshake).
    pub connect_timeout_secs: u64,
    /// Bounds inactivity between reads during a transfer; a long but
    /// progressing blob transfer is never capped by a total deadline.
    pub read_timeout_secs: u64,
    pub server_ca_bundle: Option<PathBuf>,
    pub mtls: Option<MtlsIdentity>,
    pub username: Option<String>,
    pub password: Option<Secret<String>>,
}

#[derive(Deserialize)]
struct RegistryClientConfigFields {
    url: String,
    #[serde(default = "RegistryClientConfig::default_max_redirect")]
    max_redirect: u8,
    #[serde(default = "RegistryClientConfig::default_connect_timeout_secs")]
    connect_timeout_secs: u64,
    #[serde(default = "RegistryClientConfig::default_read_timeout_secs")]
    read_timeout_secs: u64,
    server_ca_bundle: Option<PathBuf>,
    /// Named without `_bundle` to match the existing config key; renaming would
    /// break operator configs.
    client_certificate: Option<PathBuf>,
    client_private_key: Option<PathBuf>,
    username: Option<String>,
    password: Option<Secret<String>>,
}

impl TryFrom<RegistryClientConfigFields> for RegistryClientConfig {
    type Error = String;

    fn try_from(fields: RegistryClientConfigFields) -> Result<Self, Self::Error> {
        let mtls = match (fields.client_certificate, fields.client_private_key) {
            (Some(client_certificate), Some(client_private_key)) => Some(MtlsIdentity {
                client_certificate,
                client_private_key,
            }),
            (None, None) => None,
            _ => {
                return Err(
                    "both client_certificate and client_private_key are required for mTLS"
                        .to_string(),
                );
            }
        };
        Ok(Self {
            url: fields.url,
            max_redirect: fields.max_redirect,
            connect_timeout_secs: fields.connect_timeout_secs,
            read_timeout_secs: fields.read_timeout_secs,
            server_ca_bundle: fields.server_ca_bundle,
            mtls,
            username: fields.username,
            password: fields.password,
        })
    }
}

impl RegistryClientConfig {
    fn default_max_redirect() -> u8 {
        5
    }

    fn default_connect_timeout_secs() -> u64 {
        30
    }

    fn default_read_timeout_secs() -> u64 {
        300
    }
}

/// Resolved basic-auth credentials.
#[derive(Clone, Debug)]
pub struct BasicAuth {
    pub username: String,
    pub password: Secret<String>,
}

/// A blob body streamed from an upstream. `content_range` carries the window
/// the upstream says it served when it answered a ranged fetch with `206`; when
/// it is absent the reader carries the whole blob and `length` its full size.
pub struct FetchedBlob {
    pub length: u64,
    pub content_range: Option<ResponseRange>,
    pub reader: BoxedReader,
}

#[derive(Debug)]
pub struct RegistryClient {
    pub url: String,
    client: Client,
    basic_auth: Option<BasicAuth>,
    cache: Arc<Cache>,
    token_refresh: Mutex<()>,
    max_manifest_size_bytes: usize,
}

impl RegistryClient {
    /// Configured basic-auth username, `None` when the client is anonymous.
    /// Scopes cached bearer tokens so clients never share across identities.
    fn auth_username(&self) -> Option<&str> {
        self.basic_auth.as_ref().map(|auth| auth.username.as_str())
    }

    /// A client over the base `url`: `client` carries the resolved
    /// TLS/redirect/timeout policy and `cache` is the shared token/auth cache.
    #[must_use]
    pub fn new(
        url: String,
        client: Client,
        basic_auth: Option<BasicAuth>,
        cache: Arc<Cache>,
        max_manifest_size_bytes: usize,
    ) -> Self {
        Self {
            url,
            client,
            basic_auth,
            cache,
            token_refresh: Mutex::new(()),
            max_manifest_size_bytes,
        }
    }

    /// Resolves the HTTP client (TLS, redirects, timeout) and basic-auth
    /// credentials from a parsed [`RegistryClientConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Initialization`] when the TLS files cannot be loaded or
    /// the HTTP client cannot be built.
    fn resolve_config_fields(
        config: &RegistryClientConfig,
    ) -> Result<(Client, Option<BasicAuth>), Error> {
        // No whole-transfer deadline: a connect bound plus a per-read stall
        // bound so replicating a large blob is not capped by total time.
        let client = MtlsClientBuilder::new()
            .with_redirect_policy(Policy::limited(config.max_redirect as usize))
            .with_connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .with_read_timeout(Duration::from_secs(config.read_timeout_secs))
            .with_server_ca_bundle(config.server_ca_bundle.as_deref())
            .with_client_certificate((
                config.mtls.as_ref().map(|m| m.client_certificate.as_path()),
                config.mtls.as_ref().map(|m| m.client_private_key.as_path()),
            ))
            .build()
            .map_err(Error::Initialization)?;

        let basic_auth = match (&config.username, &config.password) {
            (Some(username), Some(password)) => Some(BasicAuth {
                username: username.clone(),
                password: password.clone(),
            }),
            (Some(_), None) | (None, Some(_)) => {
                warn!("Username and password must be both provided");
                None
            }
            _ => None,
        };

        Ok((client, basic_auth))
    }

    /// Builds a registry client from a parsed [`RegistryClientConfig`]; the
    /// single production construction path for upstreams and replication
    /// downstreams.
    ///
    /// # Errors
    ///
    /// Returns an error when TLS files cannot be loaded or the HTTP client cannot
    /// be built.
    pub fn from_config(
        config: &RegistryClientConfig,
        cache: Arc<Cache>,
        max_manifest_size_bytes: usize,
    ) -> Result<Self, Error> {
        let (client, basic_auth) = Self::resolve_config_fields(config)?;

        Ok(Self::new(
            config.url.clone(),
            client,
            basic_auth,
            cache,
            max_manifest_size_bytes,
        ))
    }

    async fn query(
        &self,
        method: &Method,
        accepted_types: &[MediaRange],
        location: &str,
        range: Option<&HeaderValue>,
    ) -> Result<Response, Error> {
        debug!("Requesting from upstream: {}", without_query(location));

        Ok(self
            .send_with_auth_retry_capturing(location, |auth| async move {
                self.send(method, accepted_types, location, auth.as_deref(), range)
                    .await
            })
            .await?
            .0)
    }

    /// Cached-token-then-single-refresh-retry orchestration for replayable-body
    /// requests, also returning the auth header which produced the final
    /// response: a streamed `PATCH` to a server-assigned upload-session URL
    /// reuses it, as that URL never issues its own challenge and a consumed
    /// stream cannot be replayed to refresh a token. `send_once` may run twice
    /// (cached header, then one refreshed token on `401`), so it must clone any
    /// captured-by-value request state per attempt.
    async fn send_with_auth_retry_capturing<F, Fut>(
        &self,
        location: &str,
        send_once: F,
    ) -> Result<(Response, Option<String>), Error>
    where
        F: Fn(Option<String>) -> Fut,
        Fut: Future<Output = Result<Response, Error>>,
    {
        let cached_auth = self.cached_auth_header(location).await;
        let response = send_once(cached_auth.clone()).await?;

        if response.status() == StatusCode::UNAUTHORIZED {
            let token = self
                .refresh_auth_header(&response, cached_auth.as_deref())
                .await?;
            let response = send_once(Some(token.clone())).await?;
            // A token that was just obtained will not become valid on another
            // attempt, so this is terminal rather than a response to hand back.
            if response.status() == StatusCode::UNAUTHORIZED {
                return Err(Error::Unauthorized(
                    "Upstream rejected a freshly obtained token".to_string(),
                ));
            }
            denied_if_forbidden(&response)?;
            return Ok((response, Some(token)));
        }

        denied_if_forbidden(&response)?;
        Ok((response, cached_auth))
    }

    async fn cached_auth_header(&self, location: &str) -> Option<String> {
        let url = match Url::parse(location) {
            Ok(url) => url,
            Err(e) => {
                warn!("Unable to parse upstream URL for auth cache lookup: {e}");
                return None;
            }
        };

        self.cached_auth_header_for_url(&url).await
    }

    async fn refresh_auth_header(
        &self,
        response: &Response,
        attempted_auth: Option<&str>,
    ) -> Result<String, Error> {
        let _guard = self.token_refresh.lock().await;

        if let Some(auth_header) = self.cached_auth_header_for_url(response.url()).await
            && Some(auth_header.as_str()) != attempted_auth
        {
            return Ok(auth_header);
        }

        self.authenticate_with_cache(response, attempted_auth).await
    }

    async fn cached_auth_header_for_url(&self, url: &Url) -> Option<String> {
        let index_key = match token_index_cache_key(url, self.auth_username()) {
            Ok(key) => key,
            Err(e) => {
                warn!("Unable to build auth cache key: {e}");
                return None;
            }
        };

        let key = match self.cache.retrieve_value(&index_key).await {
            Ok(Some(key)) => key,
            Ok(None) => return None,
            Err(e) => {
                warn!("Unable to read upstream auth cache index: {e}");
                return None;
            }
        };

        self.cached_auth_header_for_key(&key).await
    }

    async fn cached_auth_header_for_key(&self, key: &str) -> Option<String> {
        match self.cache.retrieve_value(key).await {
            Ok(auth_header) => auth_header,
            Err(e) => {
                warn!("Unable to read upstream auth cache: {e}");
                None
            }
        }
    }

    async fn send(
        &self,
        method: &Method,
        accepted_types: &[MediaRange],
        location: &str,
        auth_header: Option<&str>,
        range: Option<&HeaderValue>,
    ) -> Result<Response, Error> {
        self.build_request(method, accepted_types, location, auth_header, range)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("HTTP request failed: {e}")))
    }

    fn build_request(
        &self,
        method: &Method,
        accepted_types: &[MediaRange],
        location: &str,
        auth_header: Option<&str>,
        range: Option<&HeaderValue>,
    ) -> RequestBuilder {
        let mut request = self.client.request(method.clone(), location);
        for accepted_type in accepted_types {
            request = request.header(ACCEPT, accepted_type.as_str());
        }
        if let Some(auth) = auth_header {
            request = request.header(AUTHORIZATION, auth);
        }
        if let Some(range) = range {
            request = request.header(RANGE, range);
        }
        request
    }

    /// Sends a HEAD request for a blob and returns its digest and size.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, or reports that the blob is unknown.
    pub async fn head_blob(&self, request: HeadBlobRequest) -> Result<(Digest, u64), Error> {
        let location = client::blob_path(&self.url, &request.namespace, &request.digest);
        let response = self
            .query(&Method::HEAD, &request.accepted_types, &location, None)
            .await?;

        if !response.status().is_success() {
            return Err(classify_read_failure(
                response.status(),
                "head_blob",
                Error::BlobUnknown,
            ));
        }

        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST)?;
        let size = parse_header(&response, CONTENT_LENGTH)?;

        Ok((digest, size))
    }

    /// HEAD-probes a blob for presence only: `Ok(true)` on any 2xx, `Ok(false)`
    /// on `404`, and an error on any other status or transport failure.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the downstream returns a
    /// non-success status other than `404`.
    pub async fn blob_exists(&self, request: HeadBlobRequest) -> Result<bool, Error> {
        // Unlike `head_blob` this never reads `Docker-Content-Digest`, which the
        // OCI spec makes a SHOULD on blob HEAD: a conformant downstream that
        // omits it must read as present, not as a probe failure.
        let location = client::blob_path(&self.url, &request.namespace, &request.digest);
        let response = self
            .query(&Method::HEAD, &request.accepted_types, &location, None)
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(true);
        }
        if status == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        Err(Error::Internal(format!(
            "blob_exists: downstream returned status {status}"
        )))
    }

    /// Lists every tag of a repository on the downstream, following `Link`
    /// rel="next" pagination.
    ///
    /// A `404` (repository absent downstream) yields an empty list rather than
    /// an error.
    ///
    /// # Errors
    ///
    /// Returns an error when a request fails, access is rejected, or a non-404
    /// page has a non-success status or unparseable body.
    #[instrument(skip(self))]
    pub async fn list_tags(&self, request: ListTagsRequest) -> Result<Vec<Tag>, Error> {
        let location = client::tags_list_path(&self.url, &request);
        self.list_paginated(&location, "list_tags", |body| {
            let parsed: TagsListResponse = serde_json::from_slice(body)
                .map_err(|e| Error::Internal(format!("failed to parse tags/list body: {e}")))?;
            Ok(parsed
                .tags
                .into_iter()
                .filter_map(|name| match Tag::try_from(name) {
                    Ok(tag) => Some(tag),
                    Err(e) => {
                        warn!("ignoring invalid tag in downstream tags/list: {e}");
                        None
                    }
                })
                .collect())
        })
        .await
    }

    /// Lists a subject's referrers on the remote, following `Link` rel="next"
    /// pagination. A remote that serves no referrers for the subject, or does
    /// not implement the endpoint, yields an empty list rather than an error.
    ///
    /// # Errors
    ///
    /// Returns an error when a request fails, access is rejected, or a non-404
    /// page has a non-success status or unparseable body.
    #[instrument(skip(self))]
    pub async fn list_referrers(
        &self,
        request: GetReferrersRequest,
    ) -> Result<Vec<Descriptor>, Error> {
        let location = client::referrers_path(&self.url, &request);
        self.list_paginated(&location, "list_referrers", |body| {
            // The listing is an image index, which is what the serving side
            // builds; anything else is not a referrers answer.
            match Manifest::from_slice(body).map(|index| index.content) {
                Ok(Content::Index { manifests }) => Ok(manifests),
                _ => Err(Error::Internal(
                    "failed to parse referrers body as an image index".to_string(),
                )),
            }
        })
        .await
    }

    /// Walks a paginated listing from `location`, accumulating what `parse`
    /// reads out of each page body. `op` names the listing in errors.
    async fn list_paginated<T, F>(
        &self,
        location: &str,
        op: &str,
        parse: F,
    ) -> Result<Vec<T>, Error>
    where
        F: Fn(&[u8]) -> Result<Vec<T>, Error>,
    {
        // Guards against non-terminating pagination: `visited` stops an exact
        // page-URL cycle; `MAX_PAGES` backstops endless distinct pages.
        const MAX_PAGES: usize = 10_000;

        let mut items: Vec<T> = Vec::new();
        let mut next = Some(location.to_string());
        let mut visited = HashSet::new();

        while let Some(page_location) = next.take() {
            if visited.len() >= MAX_PAGES {
                return Err(Error::Internal(format!(
                    "{op} exceeded {MAX_PAGES} pages; remote pagination does not terminate"
                )));
            }
            if !visited.insert(page_location.clone()) {
                // Cyclic `rel="next"`: stop with what was gathered so far (a
                // partial list only under-reports).
                break;
            }

            let response = self.query(&Method::GET, &[], &page_location, None).await?;

            if response.status() == StatusCode::NOT_FOUND {
                return Ok(items);
            }
            if !response.status().is_success() {
                return Err(Error::Internal(format!(
                    "{op} failed with status {}",
                    response.status()
                )));
            }

            next = Self::parse_next_link(&response);

            let body = response
                .bytes()
                .await
                .map_err(|e| Error::Internal(format!("failed to read {op} body: {e}")))?;
            items.extend(parse(&body)?);
        }

        Ok(items)
    }

    /// Extracts the `Link` rel="next" continuation URL (`<...>; rel="next"`),
    /// resolved against the response's final URL, or `None` when absent.
    fn parse_next_link(response: &Response) -> Option<String> {
        let header = response.headers().get(LINK)?.to_str().ok()?;
        let target = client::next_page_target(header)?;
        let base = response.url();
        let joined = base.join(target).ok()?;
        // A cross-origin next link would send the paging request, and its auth,
        // to a host the upstream chose; stop paging rather than follow it.
        if joined.origin() != base.origin() {
            warn!("refusing cross-origin pagination Link '{joined}' from {base}");
            return None;
        }
        Some(joined.to_string())
    }

    /// Streams a blob from the upstream registry, optionally under `range` (a
    /// `Range` header value).
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, cannot satisfy `range`, or reports that the blob is unknown.
    pub async fn get_blob(&self, request: GetBlobRequest) -> Result<FetchedBlob, Error> {
        let location = client::blob_path(&self.url, &request.namespace, &request.digest);
        // A formatted range is always a valid header value, so a failure here is
        // a bug rather than anything the upstream did.
        let range = request
            .range
            .map(HeaderValue::try_from)
            .transpose()
            .map_err(|e| Error::Internal(format!("unusable Range header: {e}")))?;
        let response = self
            .query(
                &Method::GET,
                &request.accepted_types,
                &location,
                range.as_ref(),
            )
            .await?;

        if !response.status().is_success() {
            return Err(classify_read_failure(
                response.status(),
                "get_blob",
                Error::BlobUnknown,
            ));
        }

        // RFC 9110 lets a server ignore `Range` and answer `200` with the whole
        // blob, so the status is what says which body arrived. A `206` without
        // `Content-Range` is a protocol fault: serving it as whole would hand
        // the client a truncated blob.
        let content_range = match response.status() {
            StatusCode::PARTIAL_CONTENT => Some(parse_header(&response, CONTENT_RANGE)?),
            _ => None,
        };
        let length = parse_header(&response, CONTENT_LENGTH)?;
        let stream = response.bytes_stream().map_err(io::Error::other);

        Ok(FetchedBlob {
            length,
            content_range,
            reader: Box::new(StreamReader::new(stream)),
        })
    }

    /// Sends a HEAD request for a manifest and returns its metadata. The digest
    /// is absent when the upstream omits `Docker-Content-Digest`, which the OCI
    /// spec states as optional; with no body there is nothing to recompute it
    /// from, so callers decide what an unknown digest means.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, or reports that the manifest is unknown.
    pub async fn head_manifest(
        &self,
        request: HeadManifestRequest,
    ) -> Result<ManifestHeadResponse, Error> {
        let location = client::manifest_path(&self.url, &request.namespace, &request.reference);
        let response = self
            .query(&Method::HEAD, &request.accepted_types, &location, None)
            .await?;

        if !response.status().is_success() {
            return Err(classify_read_failure(
                response.status(),
                "head_manifest",
                Error::ManifestUnknown,
            ));
        }

        let media_type = response_media_type(&response);
        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST).ok();
        let size = parse_header(&response, CONTENT_LENGTH)?;

        Ok(ManifestHeadResponse {
            media_type,
            digest,
            size,
        })
    }

    /// Fetches a manifest body from the upstream registry. The digest is absent
    /// when the upstream omits `Docker-Content-Digest`, which the OCI spec
    /// states as optional; the body is authoritative, so the caller recomputes
    /// it under the algorithm its reference asked for.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, reports that the manifest is unknown, or the response body cannot be read.
    pub async fn get_manifest(
        &self,
        request: GetManifestRequest,
    ) -> Result<ManifestResponse, Error> {
        let location = client::manifest_path(&self.url, &request.namespace, &request.reference);
        let response = self
            .query(&Method::GET, &request.accepted_types, &location, None)
            .await?;

        if !response.status().is_success() {
            return Err(classify_read_failure(
                response.status(),
                "get_manifest",
                Error::ManifestUnknown,
            ));
        }

        let media_type = response_media_type(&response);
        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST).ok();

        let limit = self.max_manifest_size_bytes;
        let known_size = response.content_length();
        if known_size.is_some_and(|size| size > limit as u64) {
            return Err(Error::ManifestBodyTooLarge { limit });
        }

        let capacity = known_size
            .and_then(|size| usize::try_from(size).ok())
            .map(|size| size.min(limit))
            .unwrap_or_default();

        let stream = response.bytes_stream().map_err(io::Error::other);
        let mut content = Vec::with_capacity(capacity);
        let mut reader = StreamReader::new(stream).take(limit as u64 + 1);
        reader
            .read_to_end(&mut content)
            .await
            .map_err(|e| Error::Internal(format!("failed to read manifest body: {e}")))?;

        if content.len() > limit {
            return Err(Error::ManifestBodyTooLarge { limit });
        }

        Ok(ManifestResponse {
            media_type,
            digest,
            body: content,
        })
    }
}
