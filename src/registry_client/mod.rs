#[cfg(test)]
mod tests;

mod auth;
mod upstream_url;
mod write;

use std::{future::Future, io, path::Path, sync::Arc, time::Duration};

use auth::token_index_cache_key;
use futures_util::TryStreamExt;
use reqwest::{
    Client, Method, RequestBuilder, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
};
use serde::Deserialize;
use tokio::{io::AsyncReadExt, sync::Mutex};
use tokio_util::io::StreamReader;
use tracing::{info, warn};
pub use upstream_url::{NO_LOCAL_PREFIX, get_upstream_namespace};

pub use crate::registry_client::write::{DeleteManifestOutcome, PutManifestResult, UploadSession};

use crate::{
    cache::Cache,
    http_client::HttpClientBuilder,
    oci::Digest,
    registry::{
        DOCKER_CONTENT_DIGEST, Error, blob_store::BoxedReader,
        manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
    },
    secret::Secret,
};

/// Classifies a non-success read status: only a true 404 maps to `not_found`, so
/// callers can tell a genuinely absent object from a transient probe failure.
fn classify_read_failure(status: StatusCode, op: &str, not_found: Error) -> Error {
    if status == StatusCode::NOT_FOUND {
        not_found
    } else {
        Error::Internal(format!("{op}: downstream returned status {status}"))
    }
}

fn parse_header<T: std::str::FromStr>(
    response: &Response,
    header: impl reqwest::header::AsHeaderName,
) -> Result<T, Error> {
    response
        .headers()
        .get(header)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse().ok())
        .ok_or(Error::Unsupported)
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(try_from = "RegistryClientConfigFields")]
pub struct RegistryClientConfig {
    pub url: String,
    pub max_redirect: u8,
    pub server_ca_bundle: Option<String>,
    /// Note: named `client_certificate` (without `_bundle`) to match the existing config key;
    /// renaming would break operator configs.
    pub client_certificate: Option<String>,
    pub client_private_key: Option<String>,
    pub username: Option<String>,
    pub password: Option<Secret<String>>,
}

#[derive(Deserialize)]
struct RegistryClientConfigFields {
    url: String,
    #[serde(default = "RegistryClientConfig::default_max_redirect")]
    max_redirect: u8,
    server_ca_bundle: Option<String>,
    client_certificate: Option<String>,
    client_private_key: Option<String>,
    username: Option<String>,
    password: Option<Secret<String>>,
}

impl TryFrom<RegistryClientConfigFields> for RegistryClientConfig {
    type Error = String;

    fn try_from(fields: RegistryClientConfigFields) -> Result<Self, Self::Error> {
        if fields.client_certificate.is_some() != fields.client_private_key.is_some() {
            return Err(
                "both client_certificate and client_private_key are required for mTLS".to_string(),
            );
        }
        Ok(Self {
            url: fields.url,
            max_redirect: fields.max_redirect,
            server_ca_bundle: fields.server_ca_bundle,
            client_certificate: fields.client_certificate,
            client_private_key: fields.client_private_key,
            username: fields.username,
            password: fields.password,
        })
    }
}

impl RegistryClientConfig {
    fn default_max_redirect() -> u8 {
        5
    }
}

#[derive(Debug)]
pub struct RegistryClient {
    pub url: String,
    client: Client,
    basic_auth: Option<(String, String)>,
    cache: Arc<Cache>,
    token_refresh: Mutex<()>,
    max_manifest_size_bytes: usize,
}

impl RegistryClient {
    /// Starts building a registry client from individual resolved fields. The
    /// base `url`, the pre-built HTTP `client` (carrying the resolved
    /// TLS/redirect/timeout policy) and the shared token/auth `cache` are
    /// required; `basic_auth` and `max_manifest_size_bytes` are optional fluent
    /// setters on the returned builder.
    #[must_use]
    pub fn builder(url: String, client: Client, cache: Arc<Cache>) -> RegistryClientBuilder {
        RegistryClientBuilder {
            url,
            client,
            basic_auth: None,
            cache,
            max_manifest_size_bytes: None,
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
    ) -> Result<(Client, Option<(String, String)>), Error> {
        let client = HttpClientBuilder::new()
            .rustls_tls()
            .redirect(reqwest::redirect::Policy::limited(
                config.max_redirect as usize,
            ))
            .timeout(Duration::from_mins(5))
            .tls_files(
                config.server_ca_bundle.as_deref().map(Path::new),
                config.client_certificate.as_deref().map(Path::new),
                config.client_private_key.as_deref().map(Path::new),
            )
            .map_err(Error::Initialization)?
            .build()
            .map_err(Error::Initialization)?;

        let basic_auth = match (&config.username, &config.password) {
            (Some(username), Some(password)) => Some((username.clone(), password.expose().clone())),
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

        Ok(Self::builder(config.url.clone(), client, cache)
            .basic_auth(basic_auth)
            .max_manifest_size_bytes(max_manifest_size_bytes)
            .build())
    }

    async fn query(
        &self,
        method: &Method,
        accepted_types: &[String],
        location: &str,
    ) -> Result<Response, Error> {
        info!("Requesting from upstream: {location}");

        self.send_with_auth_retry(location, |auth| async move {
            self.send(method, accepted_types, location, auth.as_deref())
                .await
        })
        .await
    }

    /// Shared cached-token-then-single-refresh-retry orchestration for
    /// replayable-body requests ([`RegistryClient::query`] and
    /// [`RegistryClient::send_body`]).
    ///
    /// `send_once` may run twice (cached header, then one refreshed token on
    /// `401`), so it must clone any captured-by-value request state per attempt.
    async fn send_with_auth_retry<F, Fut>(
        &self,
        location: &str,
        send_once: F,
    ) -> Result<Response, Error>
    where
        F: Fn(Option<String>) -> Fut,
        Fut: Future<Output = Result<Response, Error>>,
    {
        Ok(self
            .send_with_auth_retry_capturing(location, send_once)
            .await?
            .0)
    }

    /// [`Self::send_with_auth_retry`] that also returns the auth header which
    /// produced the final response. A streamed `PATCH` to a server-assigned
    /// upload-session URL reuses it: that URL never issues its own auth
    /// challenge, and a consumed stream cannot be replayed to refresh a token.
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
            return Ok((response, Some(token)));
        }

        if response.status() == StatusCode::FORBIDDEN {
            return Err(Error::Denied("Access forbidden".to_string()));
        }

        Ok((response, cached_auth))
    }

    async fn cached_auth_header(&self, location: &str) -> Option<String> {
        let url = match url::Url::parse(location) {
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

    async fn cached_auth_header_for_url(&self, url: &url::Url) -> Option<String> {
        let index_key = match token_index_cache_key(url) {
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
        accepted_types: &[String],
        location: &str,
        auth_header: Option<&str>,
    ) -> Result<Response, Error> {
        self.build_request(method, accepted_types, location, auth_header)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("HTTP request failed: {e}")))
    }

    fn build_request(
        &self,
        method: &Method,
        accepted_types: &[String],
        location: &str,
        auth_header: Option<&str>,
    ) -> RequestBuilder {
        let mut request = self.client.request(method.clone(), location);
        for accepted_type in accepted_types {
            request = request.header(ACCEPT, accepted_type);
        }
        if let Some(auth) = auth_header {
            request = request.header(AUTHORIZATION, auth);
        }
        request
    }

    /// Sends a HEAD request for a blob and returns its digest and size.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, or reports that the blob is unknown.
    pub async fn head_blob(
        &self,
        accepted_types: &[String],
        location: &str,
    ) -> Result<(Digest, u64), Error> {
        let response = self.query(&Method::HEAD, accepted_types, location).await?;

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
    pub async fn blob_exists(&self, location: &str) -> Result<bool, Error> {
        // Unlike `head_blob` this never reads `Docker-Content-Digest`, which the
        // OCI spec makes a SHOULD on blob HEAD: a conformant downstream that
        // omits it must read as present, not as a probe failure.
        let response = self.query(&Method::HEAD, &[], location).await?;
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

    /// Streams a blob from the upstream registry.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, or reports that the blob is unknown.
    pub async fn get_blob(
        &self,
        accepted_types: &[String],
        location: &str,
    ) -> Result<(u64, BoxedReader), Error> {
        let response = self.query(&Method::GET, accepted_types, location).await?;

        if !response.status().is_success() {
            return Err(Error::BlobUnknown);
        }

        let total_length = parse_header(&response, CONTENT_LENGTH)?;
        let stream = response.bytes_stream().map_err(io::Error::other);
        let reader = StreamReader::new(stream);

        Ok((total_length, Box::new(reader)))
    }

    /// Sends a HEAD request for a manifest and returns its metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, or reports that the manifest is unknown.
    pub async fn head_manifest(
        &self,
        accepted_types: &[String],
        location: &str,
    ) -> Result<(Option<String>, Digest, u64), Error> {
        let response = self.query(&Method::HEAD, accepted_types, location).await?;

        if !response.status().is_success() {
            return Err(classify_read_failure(
                response.status(),
                "head_manifest",
                Error::ManifestUnknown,
            ));
        }

        let media_type = parse_header(&response, CONTENT_TYPE).ok();
        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST)?;
        let size = parse_header(&response, CONTENT_LENGTH)?;

        Ok((media_type, digest, size))
    }

    /// Fetches a manifest body from the upstream registry.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream request fails, rejects access, omits required
    /// headers, reports that the manifest is unknown, or the response body cannot be read.
    pub async fn get_manifest(
        &self,
        accepted_types: &[String],
        location: &str,
    ) -> Result<(Option<String>, Digest, Vec<u8>), Error> {
        let response = self.query(&Method::GET, accepted_types, location).await?;

        if !response.status().is_success() {
            return Err(classify_read_failure(
                response.status(),
                "get_manifest",
                Error::ManifestUnknown,
            ));
        }

        let media_type = parse_header(&response, CONTENT_TYPE).ok();
        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST)?;

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
        reader.read_to_end(&mut content).await?;

        if content.len() > limit {
            return Err(Error::ManifestBodyTooLarge { limit });
        }

        Ok((media_type, digest, content))
    }
}

/// Builder for [`RegistryClient`] taking individual resolved fields.
///
/// `url`, `client` and `cache` are required and supplied to
/// [`RegistryClient::builder`]; `basic_auth` defaults to none and
/// `max_manifest_size_bytes` defaults to [`DEFAULT_MAX_MANIFEST_SIZE_BYTES`].
pub struct RegistryClientBuilder {
    url: String,
    client: Client,
    basic_auth: Option<(String, String)>,
    cache: Arc<Cache>,
    max_manifest_size_bytes: Option<usize>,
}

impl RegistryClientBuilder {
    /// Optional resolved basic-auth credentials (`username`, `password`).
    #[must_use]
    pub fn basic_auth(mut self, basic_auth: Option<(String, String)>) -> Self {
        self.basic_auth = basic_auth;
        self
    }

    /// Maximum manifest body size accepted from the remote registry.
    #[must_use]
    pub fn max_manifest_size_bytes(mut self, max_manifest_size_bytes: usize) -> Self {
        self.max_manifest_size_bytes = Some(max_manifest_size_bytes);
        self
    }

    /// Builds the [`RegistryClient`].
    #[must_use]
    pub fn build(self) -> RegistryClient {
        RegistryClient {
            url: self.url,
            client: self.client,
            basic_auth: self.basic_auth,
            cache: self.cache,
            token_refresh: Mutex::new(()),
            max_manifest_size_bytes: self
                .max_manifest_size_bytes
                .unwrap_or(DEFAULT_MAX_MANIFEST_SIZE_BYTES),
        }
    }
}
