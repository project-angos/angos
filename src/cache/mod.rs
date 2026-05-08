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
                return Err(Error::Execution(error.clone()));
            }
            storage.data = Some(value.to_string());
            Ok(())
        }

        async fn retrieve_value(&self, _key: &str) -> Result<Option<String>, Error> {
            let storage = self.storage.lock().unwrap();
            if let Some(error) = &storage.retrieve_error {
                return Err(Error::Execution(error.clone()));
            }
            Ok(storage.data.clone())
        }

        async fn delete_value(&self, _key: &str) -> Result<(), Error> {
            let mut storage = self.storage.lock().unwrap();
            storage.data = None;
            Ok(())
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestData {
        name: String,
        value: i32,
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

    #[tokio::test]
    async fn retrieve_returns_hit_on_stored_value() {
        let cache = StubCache::new();
        let stored = TestData {
            name: "alice".to_string(),
            value: 7,
        };
        cache.set_data(Some(serde_json::to_string(&stored).unwrap()));

        let outcome: CacheOutcome<TestData> = cache.retrieve("k").await;

        let CacheOutcome::Hit(value) = outcome else {
            panic!("expected Hit, got Miss/Error");
        };
        assert_eq!(value, stored);
    }

    #[tokio::test]
    async fn retrieve_returns_miss_on_missing_key() {
        let cache = StubCache::new();
        cache.set_data(None);

        let outcome: CacheOutcome<TestData> = cache.retrieve("k").await;

        assert!(matches!(outcome, CacheOutcome::Miss));
    }

    #[tokio::test]
    async fn retrieve_returns_error_on_backend_failure() {
        let cache = StubCache::new();
        cache.set_retrieve_error(Some("backend down".to_string()));

        let outcome: CacheOutcome<TestData> = cache.retrieve("k").await;

        assert!(matches!(outcome, CacheOutcome::Error(Error::Execution(_))));
    }

    #[tokio::test]
    async fn retrieve_returns_error_on_deserialization_failure() {
        let cache = StubCache::new();
        cache.set_data(Some("not valid json".to_string()));

        let outcome: CacheOutcome<TestData> = cache.retrieve("k").await;

        assert!(matches!(outcome, CacheOutcome::Error(Error::Execution(_))));
    }

    #[test]
    fn try_from_outcome_maps_hit_and_miss_to_ok_and_error_to_err() {
        let hit: Result<Option<i32>, Error> = CacheOutcome::Hit(42_i32).try_into();
        assert_eq!(hit.unwrap(), Some(42));

        let miss: Result<Option<i32>, Error> = CacheOutcome::<i32>::Miss.try_into();
        assert_eq!(miss.unwrap(), None);

        let err: Result<Option<i32>, Error> =
            CacheOutcome::<i32>::Error(Error::Execution("boom".to_string())).try_into();
        assert!(matches!(err, Err(Error::Execution(msg)) if msg == "boom"));
    }
}
