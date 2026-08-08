use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use hyper::{
    Request,
    header::{HOST, HeaderMap, HeaderValue},
    http::{request::Parts, uri::Authority},
};
use tracing::instrument;

use crate::{
    auth::{Authenticator, Authorizer, TokenIssuer},
    cache::Cache,
    command::server::error::Error,
    configuration::{Configuration, TrustedProxy},
    identity::{Action, ClientIdentity, RequestScheme},
    oci::{Namespace, Reference},
    registry::{BlobMount, Registry},
};

pub struct ServerContext {
    authenticator: Arc<Authenticator>,
    authorizer: Arc<Authorizer>,
    token_issuer: Option<TokenIssuer>,
    trusted_proxies: Vec<TrustedProxy>,
    pub registry: Arc<Registry>,
    pub enable_ui: bool,
    pub ui_name: String,
}

impl ServerContext {
    /// Build the per-request context over the already-built `registry` and the
    /// shared `cache` the bootstrap constructed from the same configuration.
    pub fn new(
        config: &Configuration,
        cache: &Arc<Cache>,
        registry: Arc<Registry>,
    ) -> Result<Self, Error> {
        let authenticator = Arc::new(Authenticator::new(config, cache)?);
        let authorizer = Arc::new(Authorizer::new(config, cache)?);
        let token_issuer = config
            .auth
            .token_service
            .as_ref()
            .map(TokenIssuer::new)
            .transpose()?;

        Ok(Self {
            authenticator,
            authorizer,
            token_issuer,
            trusted_proxies: config.global.trusted_proxies.clone(),
            registry,
            enable_ui: config.ui.enabled,
            ui_name: config.ui.name.clone(),
        })
    }

    #[cfg(test)]
    pub fn has_event_dispatcher(&self) -> bool {
        self.registry.has_event_dispatcher()
    }

    pub fn token_issuer(&self) -> Option<&TokenIssuer> {
        self.token_issuer.as_ref()
    }

    /// The scheme and host a bearer challenge is derived from, or `None` when no
    /// token service is configured.
    ///
    /// Taken before the request is dispatched, since the realm may need the
    /// request's own host; the header itself is built on the denial path, so a
    /// served request does not pay for a challenge it discards.
    pub fn challenge_origin<B>(&self, request: &Request<B>) -> Option<(&'static str, String)> {
        self.token_issuer.as_ref()?;
        let host = request
            .headers()
            .get(HOST)
            .and_then(|host| host.to_str().ok())
            .or_else(|| request.uri().authority().map(Authority::as_str))?;

        Some((self.request_scheme(request), host.to_string()))
    }

    /// The `WWW-Authenticate` challenge pointing clients at the token endpoint,
    /// from an origin [`Self::challenge_origin`] captured.
    pub fn bearer_challenge(&self, scheme: &str, host: &str) -> Option<HeaderValue> {
        self.token_issuer.as_ref()?.challenge(scheme, host)
    }

    fn is_trusted_proxy(&self, peer: IpAddr) -> bool {
        self.trusted_proxies.iter().any(|p| p.contains(peer))
    }

    /// The scheme the client used, which behind a TLS-terminating proxy is not
    /// the scheme this server was reached on. Only a trusted peer's
    /// `X-Forwarded-Proto` is believed.
    fn request_scheme<B>(&self, request: &Request<B>) -> &'static str {
        let peer = request.extensions().get::<SocketAddr>();
        if peer.is_some_and(|peer| self.is_trusted_proxy(peer.ip()))
            && let Some(proto) = request.headers().get("X-Forwarded-Proto")
            && let Ok(proto) = proto.to_str()
            && proto.trim().eq_ignore_ascii_case("https")
        {
            return RequestScheme::Https.as_str();
        }

        request
            .extensions()
            .get::<RequestScheme>()
            .copied()
            .unwrap_or(RequestScheme::Http)
            .as_str()
    }

    #[instrument(skip(self, parts))]
    pub async fn authenticate_request(
        &self,
        parts: &Parts,
        remote_address: Option<SocketAddr>,
    ) -> Result<ClientIdentity, Error> {
        let mut identity = self
            .authenticator
            .authenticate_request(parts, remote_address)
            .await?;
        if let Some(peer) = remote_address
            && self.is_trusted_proxy(peer.ip())
            && let Some(client_ip) = resolve_forwarded_ip(&parts.headers, &self.trusted_proxies)
        {
            identity.client_ip = Some(client_ip);
        }
        Ok(identity)
    }

    #[instrument(skip(self, request, identity))]
    pub async fn authorize_request(
        &self,
        route: &Action,
        identity: &ClientIdentity,
        request: &Parts,
    ) -> Result<(), Error> {
        Ok(self
            .authorizer
            .authorize_request(route, identity, request, &self.registry)
            .await?)
    }

    /// Resolves a source namespace whose copy of the mount's blob `identity` can
    /// already read; `None` means fall back to an ordinary upload session.
    pub async fn authorize_mount_source(
        &self,
        mount: &BlobMount,
        identity: &ClientIdentity,
        request: &Parts,
    ) -> Result<Option<Namespace>, Error> {
        Ok(self
            .authorizer
            .authorize_mount_source(mount, identity, request, &self.registry)
            .await?)
    }

    pub fn is_reference_immutable(&self, namespace: &Namespace, reference: &Reference) -> bool {
        match reference {
            Reference::Tag(tag) => !self.authorizer.is_tag_mutable(
                self.registry
                    .get_repository_for_namespace(namespace)
                    .ok()
                    .map(|repository| repository.name.as_ref()),
                tag,
            ),
            Reference::Digest(_) => false,
        }
    }

    pub async fn shutdown(&self) {
        self.registry.shutdown().await;
    }
}

/// Resolves the client IP forwarded by a trusted proxy: the rightmost
/// `X-Forwarded-For` entry that is not itself a trusted proxy, else
/// `X-Real-IP`. Only proxies append entries on the right; anything further
/// left is client-supplied and must not be trusted.
fn resolve_forwarded_ip(headers: &HeaderMap, proxies: &[TrustedProxy]) -> Option<String> {
    if let Some(forwarded_for) = headers.get("X-Forwarded-For")
        && let Ok(forwarded_str) = forwarded_for.to_str()
    {
        for entry in forwarded_str.rsplit(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let is_proxy = entry
                .parse::<IpAddr>()
                .is_ok_and(|ip| proxies.iter().any(|p| p.contains(ip)));
            if !is_proxy {
                return Some(entry.to_string());
            }
        }
    }
    if let Some(real_ip) = headers.get("X-Real-IP")
        && let Ok(ip_str) = real_ip.to_str()
    {
        return Some(ip_str.trim().to_string());
    }
    None
}

#[cfg(test)]
pub mod tests;
