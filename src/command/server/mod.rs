//! `command::server` — entry point and infrastructure for the `server` subcommand.

mod command;
mod error;
mod handlers;
mod http_server;
mod listeners;
mod request_ext;
mod response_body;
mod router;
mod server_context;
mod ui;

pub use command::{Command, Options};
pub use error::Error;
pub use http_server::serve_request;
pub use request_ext::HeaderExt;
pub use server_context::ServerContext;
