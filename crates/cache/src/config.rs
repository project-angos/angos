use std::sync::Arc;

use serde::Deserialize;

use crate::{Cache, Error, memory, redis_backend};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub enum Config {
    #[default]
    #[serde(rename = "memory")]
    Memory,
    #[serde(rename = "redis")]
    Redis(redis_backend::BackendConfig),
}

impl Config {
    /// # Errors
    ///
    /// Returns [`Error`] when the Redis backend cannot be constructed.
    pub fn to_backend(&self) -> Result<Arc<Cache>, Error> {
        match self {
            Config::Redis(config) => {
                let backend = redis_backend::Backend::new(config)?;
                Ok(Arc::new(Cache::Redis(Box::new(backend))))
            }
            Config::Memory => Ok(Arc::new(Cache::Memory(memory::Backend::new()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use angos_secret::Secret;

    use crate::redis_backend::BackendConfig;

    #[tokio::test]
    async fn test_memory_backend() {
        let backend = Config::Memory.to_backend().unwrap();

        backend.store_value("k", "v", 60).await.unwrap();
        let retrieved = backend.retrieve_value("k").await.unwrap();
        assert_eq!(retrieved.as_deref(), Some("v"));
    }

    #[tokio::test]
    async fn test_redis_backend() {
        let backend = Config::Redis(BackendConfig {
            url: Secret::new("redis://localhost:6379/0".to_string()),
            key_prefix: "test_cache_config".to_string(),
        })
        .to_backend()
        .unwrap();

        backend.store_value("k", "v", 60).await.unwrap();
        let retrieved = backend.retrieve_value("k").await.unwrap();
        assert_eq!(retrieved.as_deref(), Some("v"));
    }

    // Verify that Config::Redis selects the Redis variant without actually
    // connecting (construction is lazy: no network call at `to_backend()`).
    #[test]
    fn redis_config_to_backend_constructs_without_connecting() {
        let backend = Config::Redis(BackendConfig {
            url: Secret::new("redis://localhost:6379/0".to_string()),
            key_prefix: "test:".to_string(),
        })
        .to_backend()
        .expect("Redis backend construction must succeed without a live server");
        assert!(
            matches!(&*backend, Cache::Redis(_)),
            "Config::Redis must construct the Redis backend variant, got: {backend:?}"
        );
    }
}
