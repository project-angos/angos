#[cfg(test)]
mod tests;

mod auth;
mod upstream_url;

use std::{io, path::Path, sync::Arc, time::Duration};

use auth::token_cache_key;
use futures_util::TryStreamExt;
use reqwest::{
    Client, Method, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
};
use serde::Deserialize;
use tokio::{io::AsyncReadExt, sync::Mutex};
use tokio_util::io::StreamReader;
use tracing::{info, warn};

use crate::{
    cache::Cache,
    http_client::HttpClientBuilder,
    oci::Digest,
    registry::{DOCKER_CONTENT_DIGEST, Error, blob_store::BoxedReader},
    secret::Secret,
};

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

#[derive(Clone, Debug, Deserialize)]
pub struct RegistryClientConfig {
    pub url: String,
    #[serde(default = "RegistryClientConfig::default_max_redirect")]
    pub max_redirect: u8,
    pub server_ca_bundle: Option<String>,
    pub client_certificate: Option<String>,
    pub client_private_key: Option<String>,
    pub username: Option<String>,
    pub password: Option<Secret<String>>,
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
}

impl RegistryClient {
    pub fn new(config: &RegistryClientConfig, cache: Arc<Cache>) -> Result<Self, Error> {
        let mut client_builder = HttpClientBuilder::new()
            .redirect(reqwest::redirect::Policy::limited(
                config.max_redirect as usize,
            ))
            .timeout(Duration::from_mins(5));

        if let Some(ca_bundle) = &config.server_ca_bundle {
            client_builder = client_builder
                .add_root_certificate_file(ca_bundle)
                .map_err(Error::Initialization)?;
        } else {
            client_builder = client_builder.rustls_tls();
        }

        client_builder = client_builder
            .identity_files(
                config.client_certificate.as_deref().map(Path::new),
                config.client_private_key.as_deref().map(Path::new),
            )
            .map_err(Error::Initialization)?;

        let client = client_builder.build().map_err(Error::Initialization)?;

        let basic_auth = match (&config.username, &config.password) {
            (Some(username), Some(password)) => Some((username.clone(), password.expose().clone())),
            (Some(_), None) | (None, Some(_)) => {
                warn!("Username and password must be both provided");
                None
            }
            _ => None,
        };

        Ok(Self {
            url: config.url.clone(),
            client,
            basic_auth,
            cache,
            token_refresh: Mutex::new(()),
        })
    }

    async fn query(
        &self,
        method: &Method,
        accepted_types: &[String],
        location: &str,
    ) -> Result<Response, Error> {
        info!("Requesting from upstream: {location}");

        let cached_auth = self.cached_auth_header(location).await;
        let response = self
            .send(method, accepted_types, location, cached_auth.as_deref())
            .await?;

        if response.status() == StatusCode::UNAUTHORIZED {
            let token = self
                .refresh_auth_header(&response, cached_auth.as_deref())
                .await?;
            return self
                .send(method, accepted_types, location, Some(&token))
                .await;
        }

        if response.status() == StatusCode::FORBIDDEN {
            return Err(Error::Denied("Access forbidden".to_string()));
        }

        Ok(response)
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

        self.authenticate(response).await
    }

    async fn cached_auth_header_for_url(&self, url: &url::Url) -> Option<String> {
        let key = match token_cache_key(url) {
            Ok(key) => key,
            Err(e) => {
                warn!("Unable to build auth cache key: {e}");
                return None;
            }
        };

        match self.cache.retrieve_value(&key).await {
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
        let mut request = self.client.request(method.clone(), location);
        for accepted_type in accepted_types {
            request = request.header(ACCEPT, accepted_type);
        }
        if let Some(auth) = auth_header {
            request = request.header(AUTHORIZATION, auth);
        }
        request
            .send()
            .await
            .map_err(|e| Error::Internal(format!("HTTP request failed: {e}")))
    }

    pub async fn head_blob(
        &self,
        accepted_types: &[String],
        location: &str,
    ) -> Result<(Digest, u64), Error> {
        let response = self.query(&Method::HEAD, accepted_types, location).await?;

        if !response.status().is_success() {
            return Err(Error::BlobUnknown);
        }

        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST)?;
        let size = parse_header(&response, CONTENT_LENGTH)?;

        Ok((digest, size))
    }

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

    pub async fn head_manifest(
        &self,
        accepted_types: &[String],
        location: &str,
    ) -> Result<(Option<String>, Digest, u64), Error> {
        let response = self.query(&Method::HEAD, accepted_types, location).await?;

        if !response.status().is_success() {
            return Err(Error::ManifestUnknown);
        }

        let media_type = parse_header(&response, CONTENT_TYPE).ok();
        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST)?;
        let size = parse_header(&response, CONTENT_LENGTH)?;

        Ok((media_type, digest, size))
    }

    pub async fn get_manifest(
        &self,
        accepted_types: &[String],
        location: &str,
    ) -> Result<(Option<String>, Digest, Vec<u8>), Error> {
        let response = self.query(&Method::GET, accepted_types, location).await?;

        if !response.status().is_success() {
            return Err(Error::ManifestUnknown);
        }

        let media_type = parse_header(&response, CONTENT_TYPE).ok();
        let digest = parse_header(&response, DOCKER_CONTENT_DIGEST)?;

        let mut content = Vec::new();
        let stream = response.bytes_stream().map_err(io::Error::other);
        StreamReader::new(stream).read_to_end(&mut content).await?;

        Ok((media_type, digest, content))
    }
}
