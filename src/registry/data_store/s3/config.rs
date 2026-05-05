use bytesize::ByteSize;
use serde::Deserialize;

use crate::secret::Secret;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct BackendConfig {
    pub access_key_id: Secret<String>,
    pub secret_key: Secret<String>,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
    pub multipart_copy_threshold: ByteSize,
    pub multipart_copy_chunk_size: ByteSize,
    pub multipart_copy_jobs: usize,
    pub multipart_part_size: ByteSize,
    pub multipart_uniform_parts: bool,
    pub operation_timeout_secs: u64,
    pub operation_attempt_timeout_secs: u64,
    pub max_attempts: u32,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            access_key_id: Secret::new(String::new()),
            secret_key: Secret::new(String::new()),
            endpoint: String::new(),
            bucket: String::new(),
            region: String::new(),
            key_prefix: String::new(),
            multipart_copy_threshold: ByteSize::gb(5),
            multipart_copy_chunk_size: ByteSize::mb(100),
            multipart_copy_jobs: 4,
            multipart_part_size: ByteSize::mib(50),
            multipart_uniform_parts: false,
            operation_timeout_secs: 900,
            operation_attempt_timeout_secs: 300,
            max_attempts: 3,
        }
    }
}
