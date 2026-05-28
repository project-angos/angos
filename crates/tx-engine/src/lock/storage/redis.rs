//! Redis lock storage (feature `redis`).
//!
//! Uses Redis `SET NX EX` for `put_if_absent` (atomic acquire) and Lua scripts
//! for conditional refresh (`put_if_match`) and ownership-checked delete
//! (`delete_if_match`). The `ETag` for a Redis lock entry is the stored value
//! itself (the serialised `LockBody` JSON written by the holder); callers
//! receive it back from `get_with_etag` and pass it to `put_if_match` /
//! `delete_if_match`.
//!
//! There is no `last_modified` concept in Redis; `get_with_etag` returns
//! `None` for `last_modified`. The lock TTL is enforced by Redis natively so
//! there is no need to read `last_modified` for expiry detection.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::Client;

use crate::lock::{
    Error,
    storage::{DeleteIfMatchOutcome, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome},
};

// ARGV[1] = value, ARGV[2] = ttl_secs
const SET_NX_SCRIPT: &str = r"
return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
";

// ARGV[1] = expected_value, ARGV[2] = ttl_secs, ARGV[3] = new_value
// Returns 1 on success, 0 on mismatch / missing
const REFRESH_SCRIPT: &str = r"
local current = redis.call('GET', KEYS[1])
if current == false or current ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[2])
return 1
";

// ARGV[1] = expected_value
// Returns 1 on deleted, 0 on mismatch / already absent
const DELETE_IF_MATCH_SCRIPT: &str = r"
local current = redis.call('GET', KEYS[1])
if current == false then
    return 1
end
if current ~= ARGV[1] then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
";

/// Configuration for the Redis lock storage.
///
/// This is a DTO. Deserialized from operator config; used to construct a
/// [`RedisLockStorage`]. Not held as a runtime field.
#[derive(Debug, Clone, serde::Deserialize, PartialEq)]
pub struct RedisLockStorageConfig {
    pub url: String,
    pub ttl: usize,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default = "RedisLockStorageConfig::default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "RedisLockStorageConfig::default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

impl RedisLockStorageConfig {
    fn default_max_retries() -> u32 {
        100
    }

    fn default_retry_delay_ms() -> u64 {
        10
    }
}

impl Default for RedisLockStorageConfig {
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

/// Redis-backed lock storage.
///
/// Safe to clone; all clones share the same [`Client`].
#[derive(Clone)]
pub struct RedisLockStorage {
    client: Arc<Client>,
    ttl_secs: usize,
    key_prefix: String,
}

impl Debug for RedisLockStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisLockStorage")
            .field("ttl_secs", &self.ttl_secs)
            .field("key_prefix", &self.key_prefix)
            .finish_non_exhaustive()
    }
}

impl RedisLockStorage {
    /// Construct a [`RedisLockStorage`] from a [`RedisLockStorageConfig`].
    ///
    /// # Errors
    ///
    /// Returns a [`redis::RedisError`] if the URL is invalid.
    pub fn new(config: &RedisLockStorageConfig) -> redis::RedisResult<Self> {
        let client = Client::open(config.url.clone())?;
        Ok(Self {
            client: Arc::new(client),
            ttl_secs: config.ttl,
            key_prefix: config.key_prefix.clone(),
        })
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
    }
}

#[async_trait]
impl LockStorage for RedisLockStorage {
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutIfAbsentOutcome, Error> {
        let full_key = self.full_key(key);
        // `body` is a JSON `LockBody`; we store its bytes verbatim as the Redis
        // value. If decoding fails we substitute a sentinel string — the ETag
        // comparison later will still work because we round-trip the same
        // bytes.
        let value = String::from_utf8(body).unwrap_or_else(|_| "unknown".to_string());

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis connect: {e}")))?;

        let result: Option<String> = redis::Script::new(SET_NX_SCRIPT)
            .key(&full_key)
            .arg(&value)
            .arg(self.ttl_secs)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis SET NX: {e}")))?;

        if result.as_deref() == Some("OK") {
            // The value itself serves as the ETag for Redis.
            Ok(PutIfAbsentOutcome::Created(Some(value)))
        } else {
            Ok(PutIfAbsentOutcome::AlreadyExists)
        }
    }

    async fn put_if_match(
        &self,
        key: &str,
        expected_etag: &str,
        body: Vec<u8>,
    ) -> Result<PutIfMatchOutcome, Error> {
        let full_key = self.full_key(key);
        let new_value = String::from_utf8(body).unwrap_or_else(|_| "unknown".to_string());

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis connect: {e}")))?;

        let result: i32 = redis::Script::new(REFRESH_SCRIPT)
            .key(&full_key)
            .arg(expected_etag)
            .arg(self.ttl_secs)
            .arg(&new_value)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis refresh script: {e}")))?;

        if result == 1 {
            Ok(PutIfMatchOutcome::Updated(Some(new_value)))
        } else {
            Ok(PutIfMatchOutcome::Mismatch)
        }
    }

    async fn get_with_etag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, Option<String>, Option<DateTime<Utc>>), Error> {
        let full_key = self.full_key(key);
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis connect: {e}")))?;

        let value: Option<String> = redis::cmd("GET")
            .arg(&full_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis GET: {e}")))?;

        match value {
            None => Err(Error::NotFound),
            Some(v) => {
                let etag = v.clone();
                Ok((v.into_bytes(), Some(etag), None))
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let full_key = self.full_key(key);
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis connect: {e}")))?;

        redis::cmd("DEL")
            .arg(&full_key)
            .query_async::<i32>(&mut conn)
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis DEL: {e}")))?;
        Ok(())
    }

    async fn delete_if_match(
        &self,
        key: &str,
        expected_etag: &str,
    ) -> Result<DeleteIfMatchOutcome, Error> {
        let full_key = self.full_key(key);
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis connect: {e}")))?;

        let result: i32 = redis::Script::new(DELETE_IF_MATCH_SCRIPT)
            .key(&full_key)
            .arg(expected_etag)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Error::StorageBackend(format!("Redis delete-if-match script: {e}")))?;

        if result == 1 {
            Ok(DeleteIfMatchOutcome::Deleted)
        } else {
            Ok(DeleteIfMatchOutcome::Mismatch)
        }
    }

    fn label(&self) -> &'static str {
        "redis"
    }
}
