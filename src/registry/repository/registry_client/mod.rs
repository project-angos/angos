#[cfg(test)]
mod tests;

mod bearer_token;

use std::{fs, io, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bearer_token::BearerToken;
use futures_util::TryStreamExt;
use reqwest::{
    Certificate, Client, Identity, Method, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, WWW_AUTHENTICATE},
    redirect::Policy,
};
use serde::Deserialize;
use tokio::{io::AsyncReadExt, sync::RwLock};
use tokio_util::io::StreamReader;
use tracing::{info, warn};

use crate::{
    cache::Cache,
    oci::{Digest, Reference},
    registry::{Error, blob_store::BoxedReader},
    secret::Secret,
};

pub const DOCKER_CONTENT_DIGEST: &str = "Docker-Content-Digest";

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
        let mut client_builder = Client::builder()
            .redirect(Policy::limited(config.max_redirect as usize))
            .timeout(Duration::from_secs(300));

        if let Some(ca_bundle) = &config.server_ca_bundle {
            let cert_pem = fs::read(ca_bundle)
                .map_err(|e| Error::Initialization(format!("Failed to read CA bundle: {e}")))?;
            let cert = Certificate::from_pem(&cert_pem)
                .map_err(|e| Error::Initialization(format!("Failed to parse CA bundle: {e}")))?;
            client_builder = client_builder.add_root_certificate(cert);
        } else {
            client_builder = client_builder.use_rustls_tls();
        }

        if let (Some(cert_path), Some(key_path)) =
            (&config.client_certificate, &config.client_private_key)
        {
            let cert_pem = fs::read(cert_path).map_err(|e| {
                Error::Initialization(format!("Failed to read client certificate: {e}"))
            })?;
            let key_pem = fs::read(key_path)
                .map_err(|e| Error::Initialization(format!("Failed to read client key: {e}")))?;
            let identity = Identity::from_pem(&[cert_pem, key_pem].concat()).map_err(|e| {
                Error::Initialization(format!("Failed to create client identity: {e}"))
            })?;
            client_builder = client_builder.identity(identity);
        }

        let client = client_builder
            .build()
            .map_err(|e| Error::Initialization(format!("Failed to build HTTP client: {e}")))?;

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
            return Err(Error::ManifestUnknown);
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
            return Err(Error::ManifestUnknown);
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
        let digest = parse_header(&response, crate::registry::manifest::DOCKER_CONTENT_DIGEST)?;

        let mut content = Vec::new();
        let stream = response.bytes_stream().map_err(io::Error::other);
        StreamReader::new(stream).read_to_end(&mut content).await?;

        Ok((media_type, digest, content))
    }

    async fn authenticate(&self, response: &Response) -> Result<String, Error> {
        let auth_header: String = parse_header(response, WWW_AUTHENTICATE)
            .map_err(|_| Error::Unauthorized("Missing WWW-Authenticate".to_string()))?;

        if let Some(bearer_params) = auth_header.strip_prefix("Bearer ") {
            let mut params = std::collections::HashMap::new();
            let param_regex = regex::Regex::new(r#"(\w+)="([^"]+)""#).unwrap();

            for cap in param_regex.captures_iter(bearer_params) {
                params.insert(cap[1].to_string(), cap[2].to_string());
            }

            let realm = params.remove("realm").ok_or_else(|| {
                Error::Internal("Missing realm parameter in WWW-Authenticate header".to_string())
            })?;

            let query = params
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");

            let mut req = self.client.get(format!("{realm}?{query}"));

            if let Some((user, pass)) = &self.basic_auth {
                let encoded = BASE64_STANDARD.encode(format!("{user}:{pass}"));
                req = req.header(AUTHORIZATION, format!("Basic {encoded}"));
            }

            let resp = req
                .send()
                .await
                .map_err(|e| Error::Internal(format!("Token request failed: {e}")))?;

            if !resp.status().is_success() {
                return Err(Error::Unauthorized(format!(
                    "Token acquisition failed: {}",
                    resp.status()
                )));
            }

            let bearer: BearerToken = resp
                .json()
                .await
                .map_err(|e| Error::Internal(format!("Failed to parse token response: {e}")))?;

            let token = format!("Bearer {}", bearer.token()?);

            let authority = response.url().host_str().unwrap_or("unknown");
            let cache_key = format!("auth:{authority}");
            let _ = self
                .cache
                .store_value(&cache_key, &token, bearer.ttl())
                .await;

            Ok(token)
        } else if auth_header.starts_with("Basic ") {
            let (user, pass) = self.basic_auth.as_ref().ok_or_else(|| {
                Error::Unauthorized("Basic auth required but not configured".to_string())
            })?;
            let encoded = BASE64_STANDARD.encode(format!("{user}:{pass}"));
            Ok(format!("Basic {encoded}"))
        } else {
            Err(Error::Internal(
                "Unsupported authentication scheme in WWW-Authenticate header".to_string(),
            ))
        }
    }

    fn get_upstream_namespace(local_name: &str, upstream_name: &str) -> String {
        upstream_name
            .strip_prefix(local_name)
            .unwrap_or(upstream_name)
            .trim_start_matches('/')
            .to_string()
    }

    pub fn get_manifest_path(
        &self,
        local_name: &str,
        upstream_name: &str,
        reference: &Reference,
    ) -> String {
        let namespace = Self::get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/manifests/{reference}", self.url)
    }

    pub fn get_blob_path(&self, local_name: &str, upstream_name: &str, digest: &Digest) -> String {
        let namespace = Self::get_upstream_namespace(local_name, upstream_name);
        format!("{}/v2/{namespace}/blobs/{digest}", self.url)
    }
}
