pub mod config;
pub mod dispatcher;
pub mod event;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("initialization error: {0}")]
    Initialization(String),
    #[error("dispatch error: {0}")]
    Dispatch(String),
}
