//! The `/v2/` API-version endpoint carries no domain state: its response is two
//! constant headers. That presentation now lives entirely in the server handler
//! (`crate::command::server::handlers::version`), so this module is intentionally
//! empty.
