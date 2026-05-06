//! `command::server` — entry point and infrastructure for the `server` subcommand.

pub mod command;
pub mod error;
pub mod handlers;
pub mod http_server;
pub mod listeners;
pub mod request_ext;
pub mod response_body;
pub mod router;
pub mod server_context;
pub mod sha256_hash_string;
pub mod ui;

pub use command::{Command, Options};
pub use error::Error;
pub use http_server::serve_request;
pub use server_context::ServerContext;
pub use sha256_hash_string::sha256_hash;
