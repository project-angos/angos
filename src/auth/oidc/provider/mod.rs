pub mod generic;
pub mod github;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::command::server::Error;

#[async_trait]
pub trait OidcProvider: Send + Sync {
    fn issuer(&self) -> &str;

    fn jwks_uri(&self) -> Option<&str>;

    fn name(&self) -> &'static str;

    fn jwks_refresh_interval(&self) -> u64;

    fn required_audience(&self) -> Option<&str>;

    fn clock_skew_tolerance(&self) -> u64;

    fn validate_provider_claims(
        &self,
        _claims: &HashMap<String, serde_json::Value>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
