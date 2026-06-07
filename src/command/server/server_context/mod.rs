use std::{sync::Arc, time::Duration};

use hyper::http::request::Parts;
use tracing::instrument;

use crate::{
    auth::{Authenticator, Authorizer},
    command::server::error::Error,
    configuration::Configuration,
    event_webhook::{dispatcher::EventDispatcher, event::Event},
    identity::{Action, ClientIdentity},
    oci::{Namespace, Reference},
    registry::{BlobMount, Registry},
};

pub struct ServerContext {
    authenticator: Arc<Authenticator>,
    authorizer: Arc<Authorizer>,
    dispatcher: Option<Arc<EventDispatcher>>,
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

        let dispatcher = if config.event_webhook.is_empty() {
            None
        } else {
            let dispatcher = EventDispatcher::builder()
                .webhooks(config.event_webhook.clone())
                .build()?;
            Some(Arc::new(dispatcher))
        };

        Ok(Self {
            authenticator,
            authorizer,
            dispatcher,
            registry,
            enable_ui: config.ui.enabled,
            ui_name: config.ui.name.clone(),
        })
    }

    /// Whether the HTTP webhook dispatcher is wired.
    #[cfg(test)]
    pub fn has_event_dispatcher(&self) -> bool {
        self.dispatcher.is_some()
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

    /// Whether `identity` may read the blob a cross-repo mount would copy, i.e.
    /// the mount would not hand over bytes the caller could not otherwise read.
    /// `false` means the caller must fall back to an ordinary upload session.
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
                    .map(|repository| repository.name.as_str()),
                tag.as_str(),
            ),
            Reference::Digest(_) => false,
        }
    }

    /// Deliver a single event to the webhook dispatcher, if one is configured.
    ///
    /// The dispatcher applies its own per-webhook delivery policy
    /// (Required/Optional/Async) internally. With no dispatcher wired this is a
    /// no-op `Ok`.
    pub async fn dispatch_event(&self, event: &Event) -> Result<(), Error> {
        match &self.dispatcher {
            Some(dispatcher) => dispatcher.dispatch(event).await.map_err(Error::from),
            None => Ok(()),
        }
    }

    /// Deliver a batch of events to the dispatcher.
    ///
    /// Unlike a short-circuiting loop, this attempts **every** event even if an
    /// earlier delivery fails, so a single failing event never aborts the rest
    /// of the batch. The first error encountered is returned once all deliveries
    /// have been attempted, preserving the externally-observable contract that a
    /// Required-webhook failure surfaces overall while Optional/Async failures
    /// are logged inside the dispatcher.
    pub async fn dispatch_events(&self, events: &[Event]) -> Result<(), Error> {
        let mut first_error: Option<Error> = None;
        for event in events {
            if let Err(error) = self.dispatch_event(event).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub async fn shutdown_with_timeout(&self, timeout: Duration) {
        self.registry.flush_pending_writes().await;
        if let Some(dispatcher) = &self.dispatcher {
            dispatcher.shutdown_with_timeout(timeout).await;
        }
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
