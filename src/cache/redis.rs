use async_trait::async_trait;
use redis::AsyncCommands;
use serde::{Deserialize, Deserializer};
use tokio::sync::OnceCell;
use tracing::info;

use crate::cache::{Cache, Error};

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct BackendConfig {
    #[serde(deserialize_with = "validate_redis_url")]
    pub url: String,
    pub key_prefix: String,
}

/// Rejects URLs that the `redis` client would refuse at `Client::open` time.
/// Catching the error here surfaces it at config-load instead of at first connect.
fn validate_redis_url<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    redis::Client::open(s.as_str())
        .map_err(|e| serde::de::Error::custom(format!("invalid Redis URL: {e}")))?;
    Ok(s)
}

pub struct Backend {
    client: redis::Client,
    connection: OnceCell<redis::aio::MultiplexedConnection>,
    key_prefix: String,
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("key_prefix", &self.key_prefix)
            .finish_non_exhaustive()
    }
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        info!("Using Redis cache store");
        let client = redis::Client::open(config.url.as_str())?;
        Ok(Backend {
            client,
            connection: OnceCell::new(),
            key_prefix: config.key_prefix.clone(),
        })
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, Error> {
        self.connection
            .get_or_try_init(|| async {
                self.client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(Error::from)
            })
            .await
            .cloned()
    }
}

#[async_trait]
impl Cache for Backend {
    async fn store_value(&self, key: &str, value: &str, expires_in: u64) -> Result<(), Error> {
        let mut conn = self.connection().await?;
        let key = format!("{}{key}", self.key_prefix);
        Ok(conn.set_ex(key, value, expires_in).await?)
    }

    async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error> {
        let mut conn = self.connection().await?;
        let key = format!("{}{key}", self.key_prefix);
        let value: Option<String> = conn.get(key).await?;
        Ok(value)
    }

    async fn delete_value(&self, key: &str) -> Result<(), Error> {
        let mut conn = self.connection().await?;
        let key = format!("{}{key}", self.key_prefix);
        let () = conn.del(key).await?;
        Ok(())
    }
}

/// Tests in this module are integration tests that require a Redis instance
/// running at `redis://localhost:6379`. Start one with `docker-compose up -d redis`
/// before running `cargo test`. They exercise the real network path and are
/// therefore slower than unit tests.
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time;
    use uuid::Uuid;

    use super::*;

    fn make_backend(db: u8) -> Backend {
        let config = BackendConfig {
            url: format!("redis://localhost:6379/{db}"),
            key_prefix: String::new(),
        };
        Backend::new(&config).unwrap()
    }

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let config = BackendConfig {
            url: "redis://localhost:6379/0".to_string(),
            key_prefix: "test_acquire_write_lock".to_owned(),
        };
        let cache = Backend::new(&config).unwrap();

        cache.store_value("key", "token", 1).await.unwrap();
        assert_eq!(
            cache.retrieve_value("key").await,
            Ok(Some("token".to_string()))
        );

        time::sleep(Duration::from_millis(1050)).await;
        assert_eq!(cache.retrieve_value("key").await, Ok(None));
    }

    /// Retrieve a key that was never stored must return `None`, not an error.
    #[tokio::test]
    async fn test_missing_key_returns_none() {
        let cache = make_backend(1);
        let key = format!("test_missing_{}", Uuid::new_v4());

        let result = cache.retrieve_value(&key).await;
        assert_eq!(result, Ok(None));
    }

    /// A multi-kilobyte value must survive a store/retrieve round-trip intact.
    #[tokio::test]
    async fn test_large_value_round_trip() {
        let cache = make_backend(1);
        let key = format!("test_large_{}", Uuid::new_v4());
        // 64 KiB of printable ASCII
        let large_value: String = "x".repeat(65_536);

        cache.store_value(&key, &large_value, 10).await.unwrap();
        let retrieved = cache.retrieve_value(&key).await.unwrap();
        // Best-effort cleanup before asserting so the key is removed even on failure.
        let _ = cache.delete_value(&key).await;

        assert_eq!(retrieved, Some(large_value));
    }

    /// `delete_value` must remove an existing key so subsequent retrieval returns `None`.
    #[tokio::test]
    async fn test_delete_removes_key() {
        let cache = make_backend(1);
        let key = format!("test_delete_{}", Uuid::new_v4());

        cache.store_value(&key, "to_be_deleted", 60).await.unwrap();
        assert_eq!(
            cache.retrieve_value(&key).await,
            Ok(Some("to_be_deleted".to_string()))
        );

        cache.delete_value(&key).await.unwrap();
        assert_eq!(cache.retrieve_value(&key).await, Ok(None));
    }

    /// A `redis://` URL with a valid host and port deserializes successfully.
    #[test]
    fn redis_url_accepts_valid_url() {
        let toml = r#"
            url = "redis://localhost:6379"
            key_prefix = "test:"
        "#;
        let result = toml::from_str::<BackendConfig>(toml);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert_eq!(result.unwrap().url, "redis://localhost:6379");
    }

    /// A `redis://` URL that selects a non-default database deserializes
    /// successfully and round-trips the input string unchanged.
    #[test]
    fn redis_url_accepts_url_with_database_path() {
        let toml = r#"
            url = "redis://localhost:6380/3"
            key_prefix = "test:"
        "#;
        let result = toml::from_str::<BackendConfig>(toml);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert_eq!(result.unwrap().url, "redis://localhost:6380/3");
    }

    /// A URL that the redis client rejects must produce a deserialization error
    /// whose message contains `"invalid Redis URL"`.
    #[test]
    fn redis_url_rejects_invalid_url() {
        let toml = r#"
            url = "not a url"
            key_prefix = "test:"
        "#;
        let result = toml::from_str::<BackendConfig>(toml);
        assert!(
            result.is_err(),
            "expected Err for invalid URL, got: {result:?}"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("invalid Redis URL"),
            "error message does not contain 'invalid Redis URL': {err_msg}"
        );
    }
}
