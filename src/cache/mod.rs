use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tracing::debug;

mod config;
mod error;
pub mod memory;
mod redis;

pub use config::Config;
pub use error::Error;

/// Three-way result of a cache lookup. Returned by [`CacheExt::retrieve`].
/// Distinguishes miss from error so the caller can decide on logging or fall-through.
pub enum CacheOutcome<T> {
    Hit(T),
    Miss,
    Error(Error),
}

impl<T> TryFrom<CacheOutcome<T>> for Option<T> {
    type Error = Error;

    /// `Hit(v) → Ok(Some(v))`, `Miss → Ok(None)`, `Error(e) → Err(e)`.
    /// Lets callers `?`-propagate backend errors while treating Hit and Miss
    /// alike as "lookup completed".
    fn try_from(outcome: CacheOutcome<T>) -> Result<Self, Self::Error> {
        match outcome {
            CacheOutcome::Hit(value) => Ok(Some(value)),
            CacheOutcome::Miss => Ok(None),
            CacheOutcome::Error(err) => Err(err),
        }
    }
}

/// Trait for cache implementations that can store and retrieve values with a given TTL
#[async_trait]
pub trait Cache: Debug + Send + Sync {
    /// Store a value with a given TTL in the cache
    async fn store_value(&self, key: &str, value: &str, expires_in: u64) -> Result<(), Error>;

    /// Retrieve a value from the cache
    async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error>;

    /// Remove a value from the cache immediately
    async fn delete_value(&self, key: &str) -> Result<(), Error>;
}

/// Extension trait providing JSON serialization for cache operations
#[async_trait]
pub trait CacheExt: Cache {
    /// Retrieve and deserialize a JSON value as a [`CacheOutcome<T>`].
    /// Distinguishes hit, miss, and backend/deserialization error so each caller
    /// can apply its own logging and fall-through policy.
    async fn retrieve<T: DeserializeOwned + Send>(&self, key: &str) -> CacheOutcome<T> {
        let cached = match self.retrieve_value(key).await {
            Ok(Some(s)) => s,
            Ok(None) => return CacheOutcome::Miss,
            Err(err) => return CacheOutcome::Error(err),
        };
        match serde_json::from_str::<T>(&cached) {
            Ok(value) => {
                debug!("Using cached value for key: {key}");
                CacheOutcome::Hit(value)
            }
            Err(e) => CacheOutcome::Error(Error::Execution(format!(
                "Failed to deserialize cached value: {e}"
            ))),
        }
    }

    /// Serialize and store a JSON value in the cache
    async fn store<T: Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
        ttl: u64,
    ) -> Result<(), Error> {
        let serialized = serde_json::to_string(value)
            .map_err(|e| Error::Execution(format!("Failed to serialize value for caching: {e}")))?;
        self.store_value(key, &serialized, ttl).await
    }
}

impl<T: Cache + ?Sized> CacheExt for T {}

#[cfg(test)]
mod tests;
