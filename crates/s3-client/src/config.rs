use bytesize::ByteSize;
use serde::Deserialize;

/// Credentials are plain `String`s: callers wanting redaction keep their own
/// secret wrapper and expose it when building this config.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct BackendConfig {
    pub access_key_id: String,
    pub secret_key: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
    pub multipart_copy_threshold: ByteSize,
    pub multipart_copy_chunk_size: ByteSize,
    pub multipart_copy_jobs: usize,
    pub multipart_part_size: ByteSize,
    pub operation_timeout_secs: u64,
    pub operation_attempt_timeout_secs: u64,
    pub max_attempts: u32,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_cooldown_secs: u64,
    #[serde(skip)]
    pub user_agent: Option<String>,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            access_key_id: String::new(),
            secret_key: String::new(),
            endpoint: String::new(),
            bucket: String::new(),
            region: String::new(),
            key_prefix: String::new(),
            multipart_copy_threshold: ByteSize::gb(5),
            multipart_copy_chunk_size: ByteSize::mb(100),
            multipart_copy_jobs: 4,
            multipart_part_size: ByteSize::mib(50),
            operation_timeout_secs: 900,
            operation_attempt_timeout_secs: 300,
            max_attempts: 3,
            circuit_breaker_threshold: 5,
            circuit_breaker_cooldown_secs: 10,
            user_agent: None,
        }
    }
}
