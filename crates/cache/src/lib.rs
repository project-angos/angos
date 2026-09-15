use serde::{Serialize, de::DeserializeOwned};

mod config;
mod error;
pub mod memory;
mod redis_backend;
#[cfg(any(test, feature = "test-util"))]
pub mod stub;

pub use config::Config;
pub use error::Error;

/// Closed-set cache backend with a configurable TTL.
///
/// Dispatches each operation to its concrete variant. Production builds expose
/// `Memory` and `Redis`; test builds add a `Stub` variant with failure injection.
#[derive(Debug)]
pub enum Cache {
    Memory(memory::Backend),
    Redis(Box<redis_backend::Backend>),
    #[cfg(any(test, feature = "test-util"))]
    Stub(stub::Backend),
}

impl Cache {
    /// # Errors
    ///
    /// Returns [`Error`] when a backend write fails.
    pub async fn store_value(&self, key: &str, value: &str, ttl: u64) -> Result<(), Error> {
        match self {
            Cache::Memory(b) => b.store_value(key, value, ttl).await,
            Cache::Redis(b) => b.store_value(key, value, ttl).await,
            #[cfg(any(test, feature = "test-util"))]
            Cache::Stub(b) => b.store_value(key, value, ttl),
        }
    }

    /// # Errors
    ///
    /// Returns [`Error`] when a backend read fails.
    pub async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error> {
        match self {
            Cache::Memory(b) => b.retrieve_value(key).await,
            Cache::Redis(b) => b.retrieve_value(key).await,
            #[cfg(any(test, feature = "test-util"))]
            Cache::Stub(b) => b.retrieve_value(key),
        }
    }

    /// # Errors
    ///
    /// Returns [`Error`] when a backend delete fails.
    pub async fn delete_value(&self, key: &str) -> Result<(), Error> {
        match self {
            Cache::Memory(b) => b.delete_value(key).await,
            Cache::Redis(b) => b.delete_value(key).await,
            #[cfg(any(test, feature = "test-util"))]
            Cache::Stub(b) => {
                b.delete_value(key);
                Ok(())
            }
        }
    }

    /// Retrieve and deserialize a JSON value. `Ok(None)` on miss, `Err` on
    /// backend or deserialization failure.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the backend read or JSON deserialization fails.
    pub async fn retrieve<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Error> {
        let Some(cached) = self.retrieve_value(key).await? else {
            return Ok(None);
        };
        let value = serde_json::from_str::<T>(&cached)
            .map_err(|e| Error::Execution(format!("Failed to deserialize cached value: {e}")))?;
        Ok(Some(value))
    }

    /// Serialize and store a JSON value in the cache.
    ///
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
