use std::fmt;

use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use serde::Deserialize;
use tokio::sync::OnceCell;
use tracing::info;
use zeroize::Zeroize;

use angos_secret::Secret;

use crate::Error;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct BackendConfig {
    /// A Redis URL carries its password in the userinfo.
    pub url: Secret<RedisUrl>,
    pub key_prefix: String,
}

/// A URL the `redis` client accepts, so a bad one fails at config load instead
/// of at first connect.
#[derive(Clone, Deserialize, PartialEq, Zeroize)]
#[serde(try_from = "String")]
pub struct RedisUrl(String);

impl TryFrom<String> for RedisUrl {
    type Error = String;

    fn try_from(url: String) -> Result<Self, Self::Error> {
        Client::open(url.as_str()).map_err(|e| format!("invalid Redis URL: {e}"))?;
        Ok(Self(url))
    }
}

pub struct Backend {
    client: Client,
    /// Reconnects on its own, so a Redis restart does not disable the cache.
    connection: OnceCell<ConnectionManager>,
    key_prefix: String,
}

impl fmt::Debug for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend")
            .field("key_prefix", &self.key_prefix)
            .finish_non_exhaustive()
    }
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using Redis cache store");
        let client = Client::open(config.url.expose().0.as_str())?;
        Ok(Backend {
            client,
            connection: OnceCell::new(),
            key_prefix: config.key_prefix.clone(),
        })
    }

    async fn connection(&self) -> Result<ConnectionManager, Error> {
        self.connection
            .get_or_try_init(|| async {
                self.client
                    .get_connection_manager()
                    .await
                    .map_err(Error::from)
            })
            .await
            .cloned()
    }

    pub async fn store_value(&self, key: &str, value: &str, ttl: u64) -> Result<(), Error> {
        let mut conn = self.connection().await?;
        let key = format!("{}{key}", self.key_prefix);
        Ok(conn.set_ex(key, value, ttl).await?)
    }

    pub async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error> {
        let mut conn = self.connection().await?;
        let key = format!("{}{key}", self.key_prefix);
        Ok(conn.get(key).await?)
    }
}

/// Most tests here need Redis at `redis://localhost:6379`.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let cache = Backend::new(&BackendConfig {
            url: Secret::new(RedisUrl::try_from("redis://localhost:6379/0".to_string()).unwrap()),
            key_prefix: String::new(),
        })
        .unwrap();
        let key = format!("test_ttl_{}", Uuid::new_v4());

        cache.store_value(&key, "token", 1).await.unwrap();
        assert_eq!(
            cache.retrieve_value(&key).await.unwrap(),
            Some("token".to_string())
        );

        time::sleep(Duration::from_millis(1050)).await;
        assert_eq!(cache.retrieve_value(&key).await.unwrap(), None);
    }

    #[test]
    fn debug_output_redacts_a_password_carried_in_the_url() {
        let toml = r#"
            url = "redis://user:hunter2@localhost:6379"
            key_prefix = "test:"
        "#;
        let config = toml::from_str::<BackendConfig>(toml).expect("valid URL");

        let debug = format!("{config:?}");
        assert!(
            !debug.contains("hunter2"),
            "Debug leaked the password: {debug}"
        );
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn redis_url_rejects_invalid_url() {
        let toml = r#"
            url = "not a url"
            key_prefix = "test:"
        "#;
        assert!(toml::from_str::<BackendConfig>(toml).is_err());
    }
}
