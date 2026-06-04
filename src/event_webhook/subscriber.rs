use std::time::Duration;

use async_trait::async_trait;

use crate::event_webhook::{Error, event::Event};

/// An internal sink for registry [`Event`]s.
///
/// Implementors receive every event the server emits and apply their own
/// delivery policy (the HTTP [`EventDispatcher`](crate::event_webhook::dispatcher::EventDispatcher)
/// applies per-webhook Required/Optional/Async policy internally). The
/// [`ServerContext`](crate::command::server::ServerContext) fans events out to
/// all registered subscribers and aggregates their errors, so a single
/// subscriber failure does not abort the rest of a batch.
#[async_trait]
pub trait EventSubscriber: Send + Sync {
    /// Deliver a single event to this subscriber.
    async fn on_event(&self, event: &Event) -> Result<(), Error>;

    /// Drain any in-flight work and stop accepting new deliveries, bounded by
    /// `timeout`.
    async fn shutdown_with_timeout(&self, timeout: Duration);
}
