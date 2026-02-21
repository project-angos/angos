use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{sync::RwLock, time::Instant};
use tracing::info;

use crate::cache::{Cache, Error};

#[derive(Debug)]
pub struct Backend {
    store: Arc<RwLock<HashMap<String, (String, Instant)>>>,
    counter: Arc<AtomicUsize>,
}

impl Backend {
    pub fn new() -> Self {
        info!("Using in-memory cache store");
        Backend {
            store: Arc::new(RwLock::new(HashMap::new())),
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn cleanup_expired(&self) {
        let mut store = self.store.write().await;
        let now = Instant::now();
        store.retain(|_, &mut (_, expiry)| expiry > now);
    }
}

#[async_trait]
impl Cache for Backend {
    async fn store_value(&self, key: &str, value: &str, expires_in: u64) -> Result<(), Error> {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);

        if count.is_multiple_of(1000) {
            self.cleanup_expired().await;
        }

        let mut store = self.store.write().await;
        store.insert(
            key.to_string(),
            (
                value.to_string(),
                Instant::now() + Duration::from_secs(expires_in),
            ),
        );
        Ok(())
    }

    async fn retrieve_value(&self, key: &str) -> Result<Option<String>, Error> {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);

        if count.is_multiple_of(1000) {
            self.cleanup_expired().await;
        }

        let store = self.store.read().await;
        if let Some((value, expiry)) = store.get(key)
            && *expiry > Instant::now()
        {
            return Ok(Some(value.clone()));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time;

    use super::*;

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let cache = Backend::new();

        let res = cache.store_value("key", "value", 1).await;
        assert!(res.is_ok());
        assert_eq!(
            cache.retrieve_value("key").await,
            Ok(Some("value".to_string()))
        );

        tokio::time::sleep(Duration::from_millis(1050)).await;
        assert_eq!(cache.retrieve_value("key").await, Ok(None));
    }

    #[tokio::test]
    async fn test_cleanup_on_counter() {
        let cache = Backend::new();

        for i in 0..500 {
            cache
                .store_value(&format!("short_{i}"), "value", 1)
                .await
                .unwrap();
        }

        for i in 0..5 {
            cache
                .store_value(&format!("long_{i}"), "value", 100)
                .await
                .unwrap();
        }

        time::sleep(Duration::from_millis(1100)).await;

        for i in 0..495 {
            let _ = cache.retrieve_value(&format!("nonexistent_{i}")).await;
        }

        assert_eq!(
            cache.retrieve_value("long_0").await,
            Ok(Some("value".to_string()))
        );
        assert_eq!(cache.retrieve_value("short_0").await, Ok(None));
    }
}
