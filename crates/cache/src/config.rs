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

    use crate::redis_backend::{BackendConfig, RedisUrl};

    /// Construction is lazy, so no live server is needed.
    #[test]
    fn redis_config_to_backend_constructs_without_connecting() {
        let backend = Config::Redis(BackendConfig {
            url: Secret::new(RedisUrl::try_from("redis://localhost:6379/0".to_string()).unwrap()),
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
