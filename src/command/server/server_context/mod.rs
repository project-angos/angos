use std::sync::Arc;

use hyper::http::request::Parts;
use tracing::instrument;

use crate::{
    auth::{Authenticator, Authorizer},
    command::server::error::Error,
    configuration::Configuration,
    identity::{Action, ClientIdentity},
    oci::{Namespace, Reference},
    registry::{BlobMount, Registry},
};

pub struct ServerContext {
    authenticator: Arc<Authenticator>,
    authorizer: Arc<Authorizer>,
    pub registry: Arc<Registry>,
    pub enable_ui: bool,
    pub ui_name: String,
}

impl ServerContext {
    pub fn new(config: &Configuration, registry: Arc<Registry>) -> Result<Self, Error> {
        let Ok(cache) = config.cache.to_backend() else {
            return Err(Error::Initialization(
                "Failed to initialize cache backend".to_string(),
            ));
        };

        let authenticator = Arc::new(Authenticator::new(config, &cache)?);
        let authorizer = Arc::new(Authorizer::new(config, &cache)?);

        Ok(Self {
            authenticator,
            authorizer,
            registry,
            enable_ui: config.ui.enabled,
            ui_name: config.ui.name.clone(),
        })
    }

    #[cfg(test)]
    pub fn has_event_dispatcher(&self) -> bool {
        self.registry.has_event_dispatcher()
    }

    #[instrument(skip(self, parts))]
    pub async fn authenticate_request(
        &self,
        parts: &Parts,
        remote_address: Option<std::net::SocketAddr>,
    ) -> Result<ClientIdentity, Error> {
        let mut identity = self
            .authenticator
            .authenticate_request(parts, remote_address)
            .await?;
        if let Some(client_ip) = resolve_client_ip(&parts.headers) {
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
        self.authorizer
            .authorize_request(route, identity, request, &self.registry)
            .await
    }

    /// Resolves a source namespace whose copy of the mount's blob `identity` can
    /// already read; `None` means fall back to an ordinary upload session.
    pub async fn authorize_mount_source(
        &self,
        mount: &BlobMount,
        identity: &ClientIdentity,
        request: &Parts,
    ) -> Result<Option<Namespace>, Error> {
        self.authorizer
            .authorize_mount_source(mount, identity, request, &self.registry)
            .await
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

fn resolve_client_ip(headers: &hyper::header::HeaderMap) -> Option<String> {
    if let Some(forwarded_for) = headers.get("X-Forwarded-For")
        && let Ok(forwarded_str) = forwarded_for.to_str()
        && let Some(first_ip) = forwarded_str.split(',').next()
    {
        return Some(first_ip.trim().to_string());
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
