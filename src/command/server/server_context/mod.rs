use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use hyper::{
    Request, Uri,
    header::{HOST, HeaderMap, HeaderValue},
    http::{request::Parts, uri::Authority},
};
use serde::Deserialize;
use tracing::instrument;

use angos_oci::request::BlobMount;
use angos_oci::{Namespace, namespace_belongs_to};

use crate::command::server::handlers::{UiConfigBody, UiOidcBody};
use crate::command::server::router::Route;

use crate::{
    auth::{Authenticator, Authorizer, TokenIssuer},
    command::server::error::Error,
    configuration::{Configuration, TrustedProxy},
    identity::{Action, ClientIdentity, RequestScheme},
    registry::{self, Registry},
};
use angos_cache::Cache;

pub struct ServerContext {
    authenticator: Arc<Authenticator>,
    authorizer: Arc<Authorizer>,
    token_issuer: Option<TokenIssuer>,
    trusted_proxies: Vec<TrustedProxy>,
    pub registry: Arc<Registry>,
    pub enable_ui: bool,
    pub ui_config: UiConfigBody,
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
        let authorizer = Arc::new(Authorizer::new(config)?);
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
            ui_config: build_ui_config(config),
        })
    }

    #[cfg(test)]
    pub fn has_event_dispatcher(&self) -> bool {
        self.registry.has_event_dispatcher()
    }

    pub fn token_issuer(&self) -> Option<&TokenIssuer> {
        self.token_issuer.as_ref()
    }

    /// Resolves a pull's proxy `?ns=` to the repository mirroring that registry
    /// namespace, rewriting `action` to address it. Returns the namespace
    /// served, which the response echoes in `OCI-Namespace`; `Ok(None)` leaves
    /// the request exactly as it arrived, which is what an unclaimed `ns`, a
    /// non-pull route, and a request already addressing that repository all get.
    ///
    /// # Errors
    ///
    /// Returns [`registry::Error::NameInvalid`] when nesting under the
    /// repository breaks the namespace length cap.
    pub fn apply_proxy_namespace(
        &self,
        route: Option<&mut Route>,
        uri: &Uri,
    ) -> Result<Option<String>, Error> {
        // The registry namespace a mirroring client believes it is addressing;
        // resolving it to a repository needs the configuration, so it happens here.
        #[derive(Deserialize)]
        struct NamespaceQuery {
            ns: Option<String>,
        }

        let Some(ns) = serde_html_form::from_str::<NamespaceQuery>(uri.query().unwrap_or_default())
            .ok()
            .and_then(|query| query.ns)
            .filter(|ns| !ns.is_empty())
        else {
            return Ok(None);
        };
        let Some(repository) = self.registry.repository_for_ns(&ns) else {
            return Ok(None);
        };
        let Some(namespace) = route.and_then(Route::pull_namespace_mut) else {
            return Ok(None);
        };

        if !namespace_belongs_to(namespace.as_ref(), repository.name.as_ref()) {
            // Refused rather than left unprefixed: serving the namespace the
            // client spelled out would answer from different content than the
            // one it asked for.
            *namespace = namespace
                .prepend(&repository.name)
                .map_err(|_| registry::Error::NameInvalid)?;
        }

        Ok(Some(ns))
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

        let scheme = request
            .extensions()
            .get::<RequestScheme>()
            .copied()
            .unwrap_or(RequestScheme::Http)
            .as_str();
        Some((scheme, host.to_string()))
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
    /// `X-Forwarded-Proto` is believed. Resolved once per request into the
    /// `RequestScheme` extension, so the bearer realm and the auth webhook can
    /// never disagree about the same request.
    pub fn resolve_scheme<B>(&self, request: &Request<B>) -> RequestScheme {
        let peer = request.extensions().get::<SocketAddr>();
        if peer.is_some_and(|peer| self.is_trusted_proxy(peer.ip()))
            && let Some(proto) = request.headers().get("X-Forwarded-Proto")
            && let Ok(proto) = proto.to_str()
            && proto.trim().eq_ignore_ascii_case("https")
        {
            return RequestScheme::Https;
        }

        request
            .extensions()
            .get::<RequestScheme>()
            .copied()
            .unwrap_or(RequestScheme::Http)
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

    /// Whether `identity` may see `namespace` in the catalog listing, under the
    /// access policies alone.
    #[must_use]
    pub fn catalog_lists_namespace(
        &self,
        namespace: &Namespace,
        identity: &ClientIdentity,
    ) -> bool {
        self.authorizer
            .allows_catalog_entry(namespace, identity, &self.registry)
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

    pub async fn shutdown(&self) {
        self.registry.shutdown().await;
    }
}

/// The document the UI reads at startup, with the sign-in provider's issuer
/// resolved from `auth.oidc`. Configuration validation rejects a provider name
/// with no such provider, so an unresolved one leaves the UI without sign-in.
fn build_ui_config(config: &Configuration) -> UiConfigBody {
    let oidc = config.ui.oidc.as_ref().and_then(|sign_in| {
        let provider = config.auth.oidc.get(&sign_in.provider)?;
        Some(UiOidcBody {
            issuer: provider.issuer.clone(),
            client_id: sign_in.client_id.clone(),
            scopes: sign_in.scopes.clone(),
        })
    });

    UiConfigBody {
        name: config.ui.name.clone(),
        oidc,
    }
}

/// Resolves the client IP forwarded by a trusted proxy: the rightmost
/// `X-Forwarded-For` entry that is not itself a trusted proxy, else
/// `X-Real-IP`. Only proxies append entries on the right; anything further
/// left is client-supplied and must not be trusted. Each candidate must parse
/// as an address, so a forged non-IP entry cannot reach the identity, and the
/// result is canonical, so a mapped IPv4 reads as its dotted form.
fn resolve_forwarded_ip(headers: &HeaderMap, proxies: &[TrustedProxy]) -> Option<String> {
    if let Some(forwarded_for) = headers.get("X-Forwarded-For")
        && let Ok(forwarded_str) = forwarded_for.to_str()
    {
        for entry in forwarded_str.rsplit(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            // A malformed entry breaks the chain: refuse to walk past it into
            // the client-supplied entries further left.
            let Ok(ip) = entry.parse::<IpAddr>() else {
                break;
            };
            if !proxies.iter().any(|p| p.contains(ip)) {
                return Some(ip.to_canonical().to_string());
            }
        }
    }
    if let Some(real_ip) = headers.get("X-Real-IP")
        && let Ok(ip_str) = real_ip.to_str()
        && let Ok(ip) = ip_str.trim().parse::<IpAddr>()
    {
        return Some(ip.to_canonical().to_string());
    }
    None
}

#[cfg(test)]
pub mod tests;
