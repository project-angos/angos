mod connection;
mod dispatch;
mod error_response;
pub mod event_emission;
mod observability;

#[cfg(test)]
mod tests;

pub use connection::serve_request;
pub use error_response::error_to_response;
