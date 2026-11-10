use serde::Serialize;
use serde::de::DeserializeOwned;

mod config;
mod error;
pub mod memory;
pub mod redis_backend;

pub use config::Config;
pub use error::Error;

/// A cache backend.
#[derive(Debug)]
pub enum Cache {
    Memory(memory::Backend),
    Redis(Box<redis_backend::Backend>),
}

impl Cache {
    /// # Errors
    ///
    /// Returns [`Error`] when a backend write fails.
    pub async fn store_value(&self, key: &str, value: &str, ttl: u64) -> Result<(), Error> {
        match self {
            Cache::Memory(b) => {
                b.store_value(key, value, ttl).await;
                Ok(())
            }
            Cache::Redis(b) => b.store_value(key, value, ttl).await,
        }
    }

    /// # Errors
    ///
    /// Returns [`Error`] when a backend read fails.
    pub async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error> {
        match self {
            Cache::Memory(b) => Ok(b.retrieve_value(key).await),
            Cache::Redis(b) => b.retrieve_value(key).await,
        }
    }

    /// # Errors
    ///
    /// Returns [`Error`] when the backend read or JSON deserialization fails.
    pub async fn retrieve<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Error> {
        let Some(cached) = self.retrieve_value(key).await? else {
            return Ok(None);
        };
        let value: T = serde_json::from_str(&cached)
            .map_err(|e| Error::Execution(format!("Failed to deserialize cached value: {e}")))?;
        Ok(Some(value))
    }

    /// # Errors
    ///
    /// Returns [`Error`] when JSON serialization or the backend write fails.
    pub async fn store<T: Serialize>(&self, key: &str, value: &T, ttl: u64) -> Result<(), Error> {
        let serialized = serde_json::to_string(value)
            .map_err(|e| Error::Execution(format!("Failed to serialize value for caching: {e}")))?;
        self.store_value(key, &serialized, ttl).await
    }
}

#[cfg(test)]
mod tests;
