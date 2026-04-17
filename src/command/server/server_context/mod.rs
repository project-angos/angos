use std::sync::Arc;

use hyper::http::request::Parts;
use tracing::instrument;

use crate::{
    command::server::{
        auth::{Authenticator, Authorizer},
        error::Error,
    },
    configuration::Configuration,
    event_webhook::{dispatcher::EventDispatcher, event::Event},
    identity::{ClientIdentity, Route},
    oci::{Namespace, Reference},
    registry::Registry,
};

pub struct ServerContext {
    authenticator: Arc<Authenticator>,
    authorizer: Arc<Authorizer>,
    event_dispatcher: Option<Arc<EventDispatcher>>,
    pub registry: Registry,
    pub enable_ui: bool,
    pub ui_name: String,
}

impl ServerContext {
    pub fn new(config: &Configuration, registry: Registry) -> Result<Self, Error> {
        let Ok(cache) = config.cache.to_backend() else {
            return Err(Error::Initialization(
                "Failed to initialize cache backend".to_string(),
            ));
        };

        let authenticator = Arc::new(Authenticator::new(config, &cache)?);
        let authorizer = Arc::new(Authorizer::new(config, &cache)?);

        let event_dispatcher = if config.event_webhook.is_empty() {
            None
        } else {
            Some(Arc::new(
                EventDispatcher::new(config.event_webhook.clone())
                    .map_err(|e| Error::Initialization(e.to_string()))?,
            ))
        };

        Ok(Self {
            authenticator,
            authorizer,
            event_dispatcher,
            registry,
            enable_ui: config.ui.enabled,
            ui_name: config.ui.name.clone(),
        })
    }

    #[cfg(test)]
    pub fn event_dispatcher(&self) -> Option<Arc<EventDispatcher>> {
        self.event_dispatcher.clone()
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
        if let Some(forwarded_for) = parts.headers.get("X-Forwarded-For")
            && let Ok(forwarded_str) = forwarded_for.to_str()
            && let Some(first_ip) = forwarded_str.split(',').next()
        {
            identity.client_ip = Some(first_ip.trim().to_string());
        } else if let Some(real_ip) = parts.headers.get("X-Real-IP")
            && let Ok(ip_str) = real_ip.to_str()
        {
            identity.client_ip = Some(ip_str.to_string());
        }

        Ok(identity)
    }

    #[instrument(skip(self, request))]
    pub async fn authorize_request(
        &self,
        route: &Route<'_>,
        identity: &ClientIdentity,
        request: &Parts,
    ) -> Result<(), Error> {
        self.authorizer
            .authorize_request(route, identity, request, &self.registry)
            .await
    }

    pub fn is_tag_immutable(&self, namespace: &str, tag: &str) -> bool {
        self.authorizer.is_tag_immutable(namespace, tag)
    }

    pub fn is_reference_immutable(&self, namespace: &Namespace, reference: &Reference) -> bool {
        match reference {
            Reference::Tag(tag) => self.is_tag_immutable(namespace, tag.as_str()),
            Reference::Digest(_) => false,
        }
    }

    pub async fn dispatch_event(&self, event: &Event) -> Result<(), Error> {
        if let Some(dispatcher) = &self.event_dispatcher {
            dispatcher.dispatch(event).await?;
        }
        Ok(())
    }

    pub async fn shutdown_with_timeout(&self, timeout: std::time::Duration) {
        self.registry.flush_pending_writes().await;
        if let Some(dispatcher) = &self.event_dispatcher {
            dispatcher.shutdown_with_timeout(timeout).await;
        }
    }
}

#[cfg(test)]
pub mod tests;
