#[cfg(test)]
mod tests;

use std::{fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::Client;
use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tracing::debug;

use crate::{
    registry::metadata_store::{
        Error,
        lock::{LockBackend, LockGuard, metrics::lock_metrics},
        simple_jitter,
    },
    timing::elapsed_ms,
};

const MAX_RETRY_DELAY_MS: u64 = 1000;

// ARGV[1] = instance_id, ARGV[2] = ttl
const ACQUIRE_SCRIPT: &str = r"
for i, key in ipairs(KEYS) do
    if redis.call('EXISTS', key) == 1 then
        return 0
    end
end
for i, key in ipairs(KEYS) do
    redis.call('SET', key, ARGV[1], 'EX', ARGV[2])
end
return 1
";

// ARGV[1] = instance_id
const RELEASE_SCRIPT: &str = r"
for i, key in ipairs(KEYS) do
    if redis.call('GET', key) == ARGV[1] then
        redis.call('DEL', key)
    end
end
return 1
";

// ARGV[1] = ttl, ARGV[2] = instance_id
const REFRESH_SCRIPT: &str = r"
for i, key in ipairs(KEYS) do
    if redis.call('GET', key) == ARGV[2] then
        redis.call('EXPIRE', key, ARGV[1])
    end
end
return 1
";

#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
pub struct LockConfig {
    pub url: String,
    pub ttl: usize,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default = "LockConfig::default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "LockConfig::default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

impl LockConfig {
    fn default_max_retries() -> u32 {
        100
    }

    fn default_retry_delay_ms() -> u64 {
        10
    }
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            ttl: 30,
            key_prefix: String::new(),
            max_retries: Self::default_max_retries(),
            retry_delay_ms: Self::default_retry_delay_ms(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisBackend {
    client: Arc<Client>,
    instance_id: String,
    ttl: usize,
    key_prefix: String,
    max_retries: u32,
    retry_delay_ms: u64,
}

impl RedisBackend {
    pub fn new(config: &LockConfig) -> redis::RedisResult<Self> {
        let client = Arc::new(Client::open(config.url.clone())?);
        Ok(RedisBackend {
            client,
            instance_id: uuid::Uuid::new_v4().to_string(),
            ttl: config.ttl,
            key_prefix: config.key_prefix.clone(),
            max_retries: config.max_retries,
            retry_delay_ms: config.retry_delay_ms,
        })
    }

    fn spawn_refresh_task(&self, keys: Vec<String>) -> (JoinHandle<()>, Arc<Notify>) {
        let client = self.client.clone();
        let ttl = self.ttl;
        let instance_id = self.instance_id.clone();
        let refresh_interval = Duration::from_secs((ttl / 2) as u64);
        let stop_notify = Arc::new(Notify::new());
        let stop_notify_clone = stop_notify.clone();

        let handle = tokio::spawn(async move {
            let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
                return;
            };
            loop {
                tokio::select! {
                    () = sleep(refresh_interval) => {
                        let _: redis::RedisResult<i32> = redis::Script::new(REFRESH_SCRIPT)
                            .key(&keys)
                            .arg(ttl)
                            .arg(&instance_id)
                            .invoke_async(&mut conn)
                            .await;
                    }
                    () = stop_notify_clone.notified() => break,
                }
            }
        });

        (handle, stop_notify)
    }

    fn retry_delay(&self, attempt: u32) -> Duration {
        let base_ms = self.retry_delay_ms.saturating_mul(1u64 << attempt.min(6));
        let capped_ms = base_ms.min(MAX_RETRY_DELAY_MS);
        let jitter = simple_jitter(capped_ms / 2);
        Duration::from_millis(capped_ms.saturating_add(jitter))
    }
}

pub struct RedisGuard {
    refresh_handle: JoinHandle<()>,
    stop_notify: Arc<Notify>,
    client: Arc<Client>,
    keys: Vec<String>,
    instance_id: String,
}

impl Drop for RedisGuard {
    fn drop(&mut self) {
        self.stop_notify.notify_one();
        self.refresh_handle.abort();

        if let Ok(mut conn) = self.client.get_connection() {
            let _: redis::RedisResult<i32> = redis::Script::new(RELEASE_SCRIPT)
                .key(&self.keys)
                .arg(&self.instance_id)
                .invoke(&mut conn);
        }
    }
}

#[async_trait]
impl LockBackend for RedisBackend {
    async fn acquire(&self, keys: &[String]) -> Result<LockGuard, Error> {
        if keys.is_empty() {
            return Ok(LockGuard::sync(Box::new(RedisGuard {
                refresh_handle: tokio::spawn(async {}),
                stop_notify: Arc::new(Notify::new()),
                client: self.client.clone(),
                keys: Vec::new(),
                instance_id: self.instance_id.clone(),
            })));
        }

        let start = std::time::Instant::now();

        let lock_keys: Vec<String> = keys
            .iter()
            .map(|k| format!("{}{}", self.key_prefix, k))
            .collect();
        let mut retries = self.max_retries;

        loop {
            let mut conn = self
                .client
                .get_multiplexed_async_connection()
                .await
                .inspect_err(|_| {
                    lock_metrics()
                        .acquisition_duration
                        .with_label_values(&["redis"])
                        .observe(elapsed_ms(start));
                    lock_metrics()
                        .acquisitions
                        .with_label_values(&["redis", "error"])
                        .inc();
                })?;

            let acquired: i32 = redis::Script::new(ACQUIRE_SCRIPT)
                .key(&lock_keys)
                .arg(&self.instance_id)
                .arg(self.ttl)
                .invoke_async(&mut conn)
                .await
                .inspect_err(|_| {
                    lock_metrics()
                        .acquisition_duration
                        .with_label_values(&["redis"])
                        .observe(elapsed_ms(start));
                    lock_metrics()
                        .acquisitions
                        .with_label_values(&["redis", "error"])
                        .inc();
                })?;

            if acquired == 1 {
                let (refresh_handle, stop_notify) = self.spawn_refresh_task(lock_keys.clone());

                lock_metrics()
                    .acquisition_duration
                    .with_label_values(&["redis"])
                    .observe(elapsed_ms(start));
                lock_metrics()
                    .acquisitions
                    .with_label_values(&["redis", "success"])
                    .inc();

                return Ok(LockGuard::sync(Box::new(RedisGuard {
                    refresh_handle,
                    stop_notify,
                    client: self.client.clone(),
                    keys: lock_keys,
                    instance_id: self.instance_id.clone(),
                })));
            }

            if retries == 0 {
                lock_metrics()
                    .acquisition_duration
                    .with_label_values(&["redis"])
                    .observe(elapsed_ms(start));
                lock_metrics()
                    .acquisitions
                    .with_label_values(&["redis", "timeout"])
                    .inc();
                return Err(Error::Lock(format!(
                    "Failed to acquire locks for keys: {keys:?}"
                )));
            }

            retries -= 1;
            let attempt = self.max_retries - retries;
            lock_metrics().retries.with_label_values(&["redis"]).inc();
            debug!("Lock busy, retrying... ({} attempts left)", retries);
            tokio::time::sleep(self.retry_delay(attempt)).await;
        }
    }
}
