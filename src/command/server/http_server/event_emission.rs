use crate::{
    command::server::{ServerContext, error::Error},
    event_webhook::event::Event,
};

/// Dispatches events produced by a handler through the server's event
/// dispatcher. Handlers are responsible for building events; the HTTP layer
/// only forwards them.
pub async fn dispatch_events(context: &ServerContext, events: Vec<Event>) -> Result<(), Error> {
    for event in events {
        context.dispatch_event(&event).await?;
    }
    Ok(())
}
