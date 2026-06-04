pub mod config;
pub mod dispatcher;
pub mod event;
pub mod subscriber;

pub use crate::event_webhook::subscriber::EventSubscriber;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("initialization error: {0}")]
    Initialization(String),
    #[error("dispatch error: {0}")]
    Dispatch(String),
}
