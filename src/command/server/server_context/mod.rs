use std::{sync::Arc, time::Duration};

use hyper::http::request::Parts;
use tracing::instrument;

use crate::{
    auth::{Authenticator, Authorizer},
    command::server::error::Error,
    configuration::Configuration,
    event_webhook::{EventSubscriber, dispatcher::EventDispatcher, event::Event},
    identity::{Action, ClientIdentity},
    oci::{Namespace, Reference},
    registry::{BlobMount, Registry},
};

pub struct ServerContext {
    authenticator: Arc<Authenticator>,
    authorizer: Arc<Authorizer>,
    subscribers: Vec<Arc<dyn EventSubscriber>>,
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

        let mut subscribers: Vec<Arc<dyn EventSubscriber>> = Vec::new();
        if !config.event_webhook.is_empty() {
            let dispatcher = EventDispatcher::builder()
                .webhooks(config.event_webhook.clone())
                .build()?;
            subscribers.push(Arc::new(dispatcher));
        }

        Ok(Self {
            authenticator,
            authorizer,
            subscribers,
            registry,
            enable_ui: config.ui.enabled,
            ui_name: config.ui.name.clone(),
        })
    }

    /// Whether any event subscriber (e.g. the HTTP webhook dispatcher) is wired.
    #[cfg(test)]
    pub fn has_event_subscribers(&self) -> bool {
        !self.subscribers.is_empty()
    }

    /// Replace the registered subscribers (tests only).
    #[cfg(test)]
    pub fn set_subscribers(&mut self, subscribers: Vec<Arc<dyn EventSubscriber>>) {
        self.subscribers = subscribers;
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

    #[instrument(skip(self, request))]
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
    ) -> Result<bool, Error> {
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

    /// Fan a single event out to every subscriber.
    ///
    /// Each subscriber applies its own delivery policy internally (the HTTP
    /// webhook dispatcher handles Required/Optional/Async per webhook). One
    /// subscriber failing does not skip the others; the first error is returned
    /// after all subscribers have been attempted.
    pub async fn dispatch_event(&self, event: &Event) -> Result<(), Error> {
        let mut first_error: Option<Error> = None;
        for subscriber in &self.subscribers {
            if let Err(error) = subscriber.on_event(event).await
                && first_error.is_none()
            {
                first_error = Some(error.into());
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Deliver a batch of events, fanning each out to every subscriber.
    ///
    /// Unlike a short-circuiting loop, this attempts **every** event against
    /// **every** subscriber even if an earlier delivery fails, so a single
    /// failing event (or subscriber) never aborts the rest of the batch. The
    /// first error encountered is returned once all deliveries have been
    /// attempted, preserving the externally-observable contract that a
    /// Required-webhook failure surfaces overall while Optional/Async failures
    /// are logged inside the subscriber.
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
        for subscriber in &self.subscribers {
            subscriber.shutdown_with_timeout(timeout).await;
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
