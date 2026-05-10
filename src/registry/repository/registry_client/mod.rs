#[cfg(test)]
mod tests;

mod auth;
mod bearer_token;
mod http_client;
mod upstream_url;

use std::{io, sync::Arc};

use futures_util::TryStreamExt;
use reqwest::{
    Client, Method, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
};
use serde::Deserialize;
use tokio::{io::AsyncReadExt, sync::RwLock};
use tokio_util::io::StreamReader;
use tracing::{info, warn};

use crate::{
    cache::Cache,
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
    cache: Arc<dyn Cache>,
    auth_cache: Arc<RwLock<Option<String>>>,
}

impl RegistryClient {
    pub fn new(config: &RegistryClientConfig, cache: Arc<dyn Cache>) -> Result<Self, Error> {
        let client = http_client::build_http_client(config)?;

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
            auth_cache: Arc::new(RwLock::new(None)),
        })
    }

    async fn query(
        &self,
        method: &Method,
        accepted_types: &[String],
        location: &str,
    ) -> Result<Response, Error> {
        info!("Requesting from upstream: {location}");

        let mut request = self.client.request(method.clone(), location);

        for accepted_type in accepted_types {
            request = request.header(ACCEPT, accepted_type);
        }

        if let Some(cached_auth) = self.auth_cache.read().await.as_ref() {
            request = request.header(AUTHORIZATION, cached_auth);
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::Internal(format!("HTTP request failed: {e}")))?;

        if response.status() == StatusCode::UNAUTHORIZED {
            let token = self.authenticate(&response).await?;
            *self.auth_cache.write().await = Some(token.clone());

            let mut retry_request = self.client.request(method.clone(), location);
            for accepted_type in accepted_types {
                retry_request = retry_request.header(ACCEPT, accepted_type);
            }
            retry_request = retry_request.header(AUTHORIZATION, &token);

            let retry_response = retry_request
                .send()
                .await
                .map_err(|e| Error::Internal(format!("HTTP request failed: {e}")))?;

            return Ok(retry_response);
        }

        if response.status() == StatusCode::FORBIDDEN {
            return Err(Error::Denied("Access forbidden".to_string()));
        }

        Ok(response)
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
