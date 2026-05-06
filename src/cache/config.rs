use std::sync::Arc;

use serde::Deserialize;

use crate::{
    cache,
    cache::{Cache, Error},
};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub enum Config {
    #[default]
    #[serde(rename = "memory")]
    Memory,
    #[serde(rename = "redis")]
    Redis(cache::redis::BackendConfig),
}

impl Config {
    pub fn to_backend(&self) -> Result<Arc<dyn Cache>, Error> {
        match self {
            Config::Redis(config) => Ok(Arc::new(cache::redis::Backend::new(config)?)),
            Config::Memory => Ok(Arc::new(cache::memory::Backend::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::redis::BackendConfig;

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
            url: "redis://localhost:6379/0".to_string(),
            key_prefix: "test_cache_config".to_string(),
        })
        .to_backend()
        .unwrap();

        backend.store_value("k", "v", 60).await.unwrap();
        let retrieved = backend.retrieve_value("k").await.unwrap();
        assert_eq!(retrieved.as_deref(), Some("v"));
    }
}
