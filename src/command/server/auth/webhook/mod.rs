#[cfg(test)]
mod tests;

use std::{
    path::PathBuf,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::Duration,
};

use hyper::{
    Uri,
    header::{HeaderName, HeaderValue},
    http::{HeaderMap, request::Parts},
};
use prometheus::{HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec};
use reqwest::{Certificate, Client, Identity, header::AUTHORIZATION, redirect::Policy};
use serde::Deserialize;
use tracing::warn;

use crate::{
    cache::{Cache, CacheExt},
    command::server::error::Error,
    identity::{ClientIdentity, Route},
    secret::Secret,
};

static WEBHOOK_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "webhook_authorization_requests_total",
        "Total webhook authorization requests",
        &["webhook", "result"]
    )
    .unwrap()
});

static WEBHOOK_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "webhook_authorization_duration_seconds",
        "Webhook authorization request duration",
        &["webhook"]
    )
    .unwrap()
});

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub url: String,
    pub timeout_ms: u64,

    #[serde(flatten)]
    pub auth: Option<WebhookAuth>,

    pub client_certificate_bundle: Option<PathBuf>,
    pub client_private_key: Option<PathBuf>,

    pub server_ca_bundle: Option<PathBuf>,

    #[serde(default)]
    pub forward_headers: Vec<String>,

    #[serde(default = "Config::default_cache_ttl")]
    pub cache_ttl: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookAuth {
    BasicAuth {
        username: String,
        password: Secret<String>,
    },
    BearerToken(Secret<String>),
}

impl Config {
    fn default_cache_ttl() -> u64 {
        60 // Default to 60 seconds
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.client_certificate_bundle.is_some() != self.client_private_key.is_some() {
            let msg = "Both certificate and key required for mTLS".to_string();
            return Err(Error::Initialization(msg));
        }

        if let Err(e) = Uri::try_from(&self.url) {
            let msg = format!("Invalid webhook URL: {e}");
            return Err(Error::Initialization(msg));
        }

        Ok(())
    }
}

pub struct WebhookAuthorizer {
    name: String,
    config: Config,
    client: Client,
    cache: Arc<dyn Cache>,
}

fn load_file(path: &PathBuf) -> Result<Vec<u8>, Error> {
    match std::fs::read(path) {
        Ok(pem) => Ok(pem),
        Err(e) => {
            let msg = format!("Failed to read certificate file: {e}");
            Err(Error::Initialization(msg))
        }
    }
}

fn load_certificate_bundle(path: &PathBuf) -> Result<Vec<Certificate>, Error> {
    let certificate_pem = load_file(path)?;

    match Certificate::from_pem_bundle(&certificate_pem) {
        Ok(cert) => Ok(cert),
        Err(e) => {
            let msg = format!("Failed to parse certificate: {e}");
            Err(Error::Initialization(msg))
        }
    }
}

fn load_identity(
    cert_path: Option<&PathBuf>,
    key_path: Option<&PathBuf>,
) -> Result<Option<Identity>, Error> {
    let (Some(cert_path), Some(key_path)) = (cert_path, key_path) else {
        return Ok(None);
    };

    let cert_pem = load_file(cert_path)?;
    let key_pem = load_file(key_path)?;

    match Identity::from_pem(&[cert_pem, key_pem].concat()) {
        Ok(identity) => Ok(Some(identity)),
        Err(e) => {
            let msg = format!("Failed to create identity from PEM: {e}");
            Err(Error::Initialization(msg))
        }
    }
}

fn build_header_name(name: &str) -> Result<HeaderName, Error> {
    match HeaderName::from_str(name) {
        Ok(h) => Ok(h),
        Err(e) => {
            let msg = format!("Invalid header name '{name}': {e}");
            Err(Error::Execution(msg))
        }
    }
}

fn build_header_value(value: &str) -> Result<HeaderValue, Error> {
    match HeaderValue::from_str(value) {
        Ok(hv) => Ok(hv),
        Err(e) => {
            let msg = format!("Invalid header value '{value}': {e}");
            Err(Error::Execution(msg))
        }
    }
}

static X_FORWARDED_METHOD: &str = "X-Forwarded-Method";
static X_FORWARDED_PROTO: &str = "X-Forwarded-Proto";
static X_FORWARDED_HOST: &str = "X-Forwarded-Host";
static X_FORWARDED_URI: &str = "X-Forwarded-Uri";
static X_FORWARDED_FOR: &str = "X-Forwarded-For";
static X_REGISTRY_ACTION: &str = "X-Registry-Action";
static X_REGISTRY_NAMESPACE: &str = "X-Registry-Namespace";
static X_REGISTRY_REFERENCE: &str = "X-Registry-Reference";
static X_REGISTRY_DIGEST: &str = "X-Registry-Digest";
static X_REGISTRY_USERNAME: &str = "X-Registry-Username";
static X_REGISTRY_IDENTITY_ID: &str = "X-Registry-Identity-ID";
static X_REGISTRY_CERTIFICATE_CN: &str = "X-Registry-Certificate-CN";
static X_REGISTRY_CERTIFICATE_O: &str = "X-Registry-Certificate-O";

fn set_forwarded_method_header(parts: &Parts, headers: &mut HeaderMap) -> Result<(), Error> {
    let value = build_header_value(parts.method.as_str())?;
    headers.insert(X_FORWARDED_METHOD, value);
    Ok(())
}

fn set_forwarded_proto_header(parts: &Parts, headers: &mut HeaderMap) -> Result<(), Error> {
    let proto = if parts.uri.scheme_str() == Some("https") {
        build_header_value("https")?
    } else {
        build_header_value("http")?
    };

    headers.insert(X_FORWARDED_PROTO, proto);
    Ok(())
}

fn set_forwarded_host_header(parts: &Parts, headers: &mut HeaderMap) {
    if let Some(host) = parts.headers.get("Host") {
        headers.insert(X_FORWARDED_HOST, host.clone());
    }
}

fn set_forwarded_uri_header(parts: &Parts, headers: &mut HeaderMap) -> Result<(), Error> {
    let uri = parts.uri.to_string();
    let value = build_header_value(&uri)?;
    headers.insert(X_FORWARDED_URI, value);
    Ok(())
}

fn set_forwarded_for_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(ip) = &identity.client_ip {
        let value = build_header_value(ip)?;
        headers.insert(X_FORWARDED_FOR, value);
    }
    Ok(())
}

fn set_registry_action_header(route: &Route<'_>, headers: &mut HeaderMap) -> Result<(), Error> {
    let value = build_header_value(route.action_name())?;
    headers.insert(X_REGISTRY_ACTION, value);
    Ok(())
}

fn set_registry_namespace_header(route: &Route<'_>, headers: &mut HeaderMap) -> Result<(), Error> {
    if let Some(namespace) = route.get_namespace() {
        let value = build_header_value(namespace)?;
        headers.insert(X_REGISTRY_NAMESPACE, value);
    }
    Ok(())
}

fn set_registry_reference_header(route: &Route<'_>, headers: &mut HeaderMap) -> Result<(), Error> {
    if let Some(reference) = route.get_reference() {
        let value = build_header_value(reference.as_str())?;
        headers.insert(X_REGISTRY_REFERENCE, value);
    }
    Ok(())
}

fn set_registry_digest_header(route: &Route<'_>, headers: &mut HeaderMap) -> Result<(), Error> {
    if let Some(digest) = route.get_digest() {
        let value = build_header_value(&digest.to_string())?;
        headers.insert(X_REGISTRY_DIGEST, value);
    }
    Ok(())
}

fn set_registry_username_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(username) = &identity.username {
        let value = build_header_value(username)?;
        headers.insert(X_REGISTRY_USERNAME, value);
    }
    Ok(())
}

fn set_registry_identity_id_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(id) = &identity.id {
        let value = build_header_value(id)?;
        headers.insert(X_REGISTRY_IDENTITY_ID, value);
    }
    Ok(())
}

fn set_registry_certificate_cn_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    for cn in &identity.certificate.common_names {
        let value = build_header_value(cn)?;
        headers.append(X_REGISTRY_CERTIFICATE_CN, value);
    }
    Ok(())
}

fn set_registry_certificate_o_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    for org in &identity.certificate.organizations {
        let value = build_header_value(org)?;
        headers.append(X_REGISTRY_CERTIFICATE_O, value);
    }
    Ok(())
}

fn set_forwarded_headers(
    forward_headers: &[String],
    parts: &Parts,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    for name in forward_headers {
        if let Some(value) = parts.headers.get(name) {
            let name = build_header_name(name)?;
            headers.insert(name, value.clone());
        }
    }
    Ok(())
}

fn build_headers(
    forward_headers: &[String],
    route: &Route<'_>,
    identity: &ClientIdentity,
    parts: &Parts,
) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::new();

    set_forwarded_method_header(parts, &mut headers)?;
    set_forwarded_proto_header(parts, &mut headers)?;
    set_forwarded_host_header(parts, &mut headers);
    set_forwarded_uri_header(parts, &mut headers)?;
    set_forwarded_for_header(identity, &mut headers)?;

    set_registry_action_header(route, &mut headers)?;
    set_registry_namespace_header(route, &mut headers)?;
    set_registry_reference_header(route, &mut headers)?;
    set_registry_digest_header(route, &mut headers)?;

    set_registry_username_header(identity, &mut headers)?;
    set_registry_identity_id_header(identity, &mut headers)?;

    set_registry_certificate_cn_header(identity, &mut headers)?;
    set_registry_certificate_o_header(identity, &mut headers)?;

    set_forwarded_headers(forward_headers, parts, &mut headers)?;
    Ok(headers)
}

fn build_cache_key(
    name: &str,
    route: &Route<'_>,
    identity: &ClientIdentity,
) -> Result<String, Error> {
    let Ok(route_json) = serde_json::to_string(route) else {
        let msg = "Failed to serialize route".to_string();
        return Err(Error::Execution(msg));
    };

    let Ok(identity_json) = serde_json::to_string(&identity) else {
        let msg = "Failed to serialize identity".to_string();
        return Err(Error::Execution(msg));
    };

    Ok(format!("webhook:{name}:{identity_json}:{route_json}"))
}

async fn cache_retrieve(
    cache: &Arc<dyn Cache>,
    name: &str,
    cache_key: &str,
) -> Result<Option<bool>, Error> {
    let Ok(Some(cached)) = cache.retrieve::<bool>(cache_key).await else {
        return Ok(None);
    };

    let label = if cached {
        "cached_allow"
    } else {
        "cached_deny"
    };

    WEBHOOK_REQUESTS.with_label_values(&[name, label]).inc();

    Ok(Some(cached))
}

impl WebhookAuthorizer {
    pub fn new(name: String, config: Config, cache: Arc<dyn Cache>) -> Result<Self, Error> {
        let mut client_builder = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_millis(config.timeout_ms));

        if let Some(ca_bundle) = &config.server_ca_bundle {
            let ca_bundle_certs = load_certificate_bundle(ca_bundle)?;
            for cert in ca_bundle_certs {
                client_builder = client_builder.add_root_certificate(cert);
            }
        }

        let identity = load_identity(
            config.client_certificate_bundle.as_ref(),
            config.client_private_key.as_ref(),
        )?;
        if let Some(identity) = identity {
            client_builder = client_builder.identity(identity);
        }

        let client = client_builder
            .build()
            .map_err(|e| Error::Initialization(format!("Failed to create HTTP client: {e}")))?;

        Ok(Self {
            name,
            config,
            client,
            cache,
        })
    }

    pub async fn authorize(
        &self,
        route: &Route<'_>,
        identity: &ClientIdentity,
        parts: &Parts,
    ) -> Result<bool, Error> {
        let cache_key = build_cache_key(&self.name, route, identity);

        if let Ok(cache_key) = &cache_key
            && let Ok(Some(cached)) = cache_retrieve(&self.cache, &self.name, cache_key).await
        {
            return Ok(cached);
        }

        let timer = WEBHOOK_DURATION
            .with_label_values(&[&self.name])
            .start_timer();

        let headers = build_headers(&self.config.forward_headers, route, identity, parts)?;
        let mut request = self.client.get(&self.config.url);
        for (key, value) in &headers {
            request = request.header(key, value);
        }

        if let Some(WebhookAuth::BearerToken(token)) = &self.config.auth {
            request = request.header(AUTHORIZATION, format!("Bearer {}", token.expose()));
        } else if let Some(WebhookAuth::BasicAuth { username, password }) = &self.config.auth {
            request = request.basic_auth(username, Some(password.expose()));
        }

        let response = request.send().await;

        let allowed = match response {
            Ok(resp) => resp.status().is_success(),
            Err(e) => {
                warn!("Webhook '{}' request failed: {e}", self.name);
                false
            }
        };

        timer.observe_duration();

        let result_label = if allowed {
            "allow".to_string()
        } else {
            "deny".to_string()
        };

        WEBHOOK_REQUESTS
            .with_label_values(&[&self.name, &result_label])
            .inc();

        if let Ok(cache_key) = &cache_key {
            let _ = self
                .cache
                .store(cache_key, &allowed, self.config.cache_ttl)
                .await;
        }

        Ok(allowed)
    }
}
