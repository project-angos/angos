#[cfg(test)]
mod tests;

use std::{fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::Client;
use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tracing::debug;

use crate::registry::metadata_store::{Error, lock::LockBackend};

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

const RELEASE_SCRIPT: &str = r"
for i, key in ipairs(KEYS) do
    redis.call('DEL', key)
end
return 1
";

const REFRESH_SCRIPT: &str = r"
for i, key in ipairs(KEYS) do
    redis.call('EXPIRE', key, ARGV[1])
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
            ttl: config.ttl,
            key_prefix: config.key_prefix.clone(),
            max_retries: config.max_retries,
            retry_delay_ms: config.retry_delay_ms,
        })
    }
}

pub struct RedisGuard {
    refresh_handle: JoinHandle<()>,
    stop_notify: Arc<Notify>,
    client: Arc<Client>,
    keys: Vec<String>,
}

impl Drop for RedisGuard {
    fn drop(&mut self) {
        self.stop_notify.notify_one();
        self.refresh_handle.abort();

        if let Ok(mut conn) = self.client.get_connection() {
            let _: redis::RedisResult<i32> = redis::Script::new(RELEASE_SCRIPT)
                .key(&self.keys)
                .invoke(&mut conn);
        }
    }
}

#[async_trait]
impl LockBackend for RedisBackend {
    type Guard = Box<dyn Send>;

    async fn acquire(&self, keys: &[String]) -> Result<Self::Guard, Error> {
        if keys.is_empty() {
            return Ok(Box::new(RedisGuard {
                refresh_handle: tokio::spawn(async {}),
                stop_notify: Arc::new(Notify::new()),
                client: self.client.clone(),
                keys: Vec::new(),
            }));
        }

        let lock_keys: Vec<String> = keys
            .iter()
            .map(|k| format!("{}{}", self.key_prefix, k))
            .collect();
        let mut retries = self.max_retries;
        let retry_delay = Duration::from_millis(self.retry_delay_ms);

        loop {
            let mut conn = self.client.get_multiplexed_async_connection().await?;

            let acquired: i32 = redis::Script::new(ACQUIRE_SCRIPT)
                .key(&lock_keys)
                .arg(1)
                .arg(self.ttl)
                .invoke_async(&mut conn)
                .await?;

            if acquired == 1 {
                let stop_notify = Arc::new(Notify::new());
                let client = self.client.clone();
                let keys_for_refresh = lock_keys.clone();
                let ttl = self.ttl;
                let refresh_interval = Duration::from_secs((ttl / 2) as u64);
                let stop_notify_clone = stop_notify.clone();

                let refresh_handle = tokio::spawn(async move {
                    let Ok(mut conn) = client.get_multiplexed_async_connection().await else {
                        return;
                    };

                    loop {
                        tokio::select! {
                            () = sleep(refresh_interval) => {
                                let _: redis::RedisResult<i32> = redis::Script::new(REFRESH_SCRIPT)
                                    .key(&keys_for_refresh)
                                    .arg(ttl)
                                    .invoke_async(&mut conn)
                                    .await;
                            }
                            () = stop_notify_clone.notified() => {
                                break;
                            }
                        }
                    }
                });

                return Ok(Box::new(RedisGuard {
                    refresh_handle,
                    stop_notify,
                    client: self.client.clone(),
                    keys: lock_keys,
                }));
            }

            if retries == 0 {
                return Err(Error::Lock(format!(
                    "Failed to acquire locks for keys: {keys:?}"
                )));
            }

            retries -= 1;
            debug!("Lock busy, retrying... ({} attempts left)", retries);
            tokio::time::sleep(retry_delay).await;
        }
    }
}
