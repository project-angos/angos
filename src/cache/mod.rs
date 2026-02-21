use std::{any::Any, fmt::Debug};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use tracing::{debug, warn};

mod config;
mod error;
pub mod memory;
mod redis;

pub use config::Config;
pub use error::Error;

/// Trait for cache implementations that can store and retrieve values with a given TTL
#[async_trait]
pub trait Cache: Any + Debug + Send + Sync {
    /// Store a value with a given TTL in the cache
    async fn store_value(&self, key: &str, value: &str, expires_in: u64) -> Result<(), Error>;

    /// Retrieve a value from the cache
    async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error>;
}

/// Extension trait providing JSON serialization for cache operations
#[async_trait]
pub trait CacheExt: Cache {
    /// Retrieve and deserialize a JSON value from the cache
    async fn retrieve<T: DeserializeOwned + Send>(&self, key: &str) -> Result<Option<T>, Error> {
        let cached = match self.retrieve_value(key).await {
            Ok(s) => s,
            Err(err) => {
                warn!("Failed to retrieve value from cache for key {key}: {err}");
                return Err(Error::Execution(format!(
                    "Failed to retrieve value from cache: {err}"
                )));
            }
        };

        let Some(cached) = cached else {
            return Ok(None);
        };

        match serde_json::from_str::<T>(&cached) {
            Ok(value) => {
                debug!("Using cached value for key: {key}");
                Ok(Some(value))
            }
            Err(e) => {
                warn!("Failed to deserialize cached value for key {key}: {e}");
                Err(Error::Execution(format!(
                    "Failed to deserialize cached value: {e}"
                )))
            }
        }
    }

    /// Serialize and store a JSON value in the cache
    async fn store<T: Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
        ttl: u64,
    ) -> Result<(), Error> {
        let serialized = match serde_json::to_string(value) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to serialize value for caching for key {key}: {e}");
                return Err(Error::Execution(format!(
                    "Failed to serialize value for caching: {e}"
                )));
            }
        };

        if let Err(err) = self.store_value(key, &serialized, ttl).await {
            warn!("Failed to store value in cache for key {key}: {err}");
            return Err(Error::Execution(format!(
                "Failed to store value in cache: {err}"
            )));
        }

        Ok(())
    }
}

impl<T: Cache + ?Sized> CacheExt for T {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug)]
    struct StubCache {
        storage: Arc<Mutex<StubStorage>>,
    }

    #[derive(Debug)]
    struct StubStorage {
        data: Option<String>,
        retrieve_error: Option<String>,
        store_error: Option<String>,
    }

    impl StubCache {
        fn new() -> Self {
            Self {
                storage: Arc::new(Mutex::new(StubStorage {
                    data: None,
                    retrieve_error: None,
                    store_error: None,
                })),
            }
        }

        fn set_data(&self, data: Option<String>) {
            self.storage.lock().unwrap().data = data;
        }

        fn set_retrieve_error(&self, error: Option<String>) {
            self.storage.lock().unwrap().retrieve_error = error;
        }

        fn set_store_error(&self, error: Option<String>) {
            self.storage.lock().unwrap().store_error = error;
        }

        fn get_stored_data(&self) -> Option<String> {
            self.storage.lock().unwrap().data.clone()
        }
    }

    #[async_trait]
    impl Cache for StubCache {
        async fn store_value(
            &self,
            _key: &str,
            value: &str,
            _expires_in: u64,
        ) -> Result<(), Error> {
            let mut storage = self.storage.lock().unwrap();
            if let Some(error) = &storage.store_error {
                return Err(Error::Backend(error.clone()));
            }
            storage.data = Some(value.to_string());
            Ok(())
        }

        async fn retrieve_value(&self, _key: &str) -> Result<Option<String>, Error> {
            let storage = self.storage.lock().unwrap();
            if let Some(error) = &storage.retrieve_error {
                return Err(Error::Backend(error.clone()));
            }
            Ok(storage.data.clone())
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[tokio::test]
    async fn test_retrieve_success() {
        let cache = StubCache::new();
        let test_data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let serialized = serde_json::to_string(&test_data).unwrap();
        cache.set_data(Some(serialized));

        let result: Result<Option<TestData>, Error> = cache.retrieve("test_key").await;

        assert!(result.is_ok());
        let retrieved = result.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), test_data);
    }

    #[tokio::test]
    async fn test_retrieve_not_found() {
        let cache = StubCache::new();
        cache.set_data(None);

        let result: Result<Option<TestData>, Error> = cache.retrieve("test_key").await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_retrieve_backend_error() {
        let cache = StubCache::new();
        cache.set_retrieve_error(Some("Backend failure".to_string()));

        let result: Result<Option<TestData>, Error> = cache.retrieve("test_key").await;

        assert!(matches!(result, Err(Error::Execution(_))));
    }

    #[tokio::test]
    async fn test_retrieve_deserialization_error() {
        let cache = StubCache::new();
        cache.set_data(Some("invalid json".to_string()));

        let result: Result<Option<TestData>, Error> = cache.retrieve("test_key").await;

        assert!(matches!(result, Err(Error::Execution(_))));
    }

    #[tokio::test]
    async fn test_store_success() {
        let cache = StubCache::new();
        let test_data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let result = cache.store("test_key", &test_data, 60).await;

        assert!(result.is_ok());

        let stored = cache.get_stored_data();
        assert!(stored.is_some());
        let deserialized: TestData = serde_json::from_str(&stored.unwrap()).unwrap();
        assert_eq!(deserialized, test_data);
    }

    #[tokio::test]
    async fn test_store_backend_error() {
        let cache = StubCache::new();
        cache.set_store_error(Some("Backend failure".to_string()));

        let test_data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let result = cache.store("test_key", &test_data, 60).await;

        assert!(matches!(result, Err(Error::Execution(_))));
    }

    #[derive(Debug, Serialize)]
    struct UnserializableData {
        #[serde(serialize_with = "fail_serialization")]
        value: i32,
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn fail_serialization<S>(_: &i32, _: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom(
            "Intentional serialization failure",
        ))
    }

    #[tokio::test]
    async fn test_store_serialization_error() {
        let cache = StubCache::new();
        let bad_data = UnserializableData { value: 42 };

        let result = cache.store("test_key", &bad_data, 60).await;

        assert!(matches!(result, Err(Error::Execution(_))));
    }
}
