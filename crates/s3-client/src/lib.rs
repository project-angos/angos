//! S3-compatible storage client. Nothing HTTP- or XML-specific is exported.

use std::future::Future;

use bytesize::ByteSize;

use crate::circuit_breaker::CircuitBreaker;
use crate::client::S3Client;

mod circuit_breaker;
mod client;
mod config;
mod error;
mod ops;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
mod xml;

pub use crate::config::BackendConfig;
pub use crate::error::Error;
pub use crate::ops::UploadedPart;

#[derive(Clone, Debug)]
pub struct Backend {
    s3_client: S3Client,
    key_prefix: String,
    multipart_copy_threshold: u64,
    multipart_copy_chunk_size: u64,
    multipart_copy_jobs: usize,
    circuit_breaker: CircuitBreaker,
}

impl Backend {
    /// Build a new backend from `config`.
    ///
    /// # Errors
    /// Returns [`Error::Configuration`] when multipart sizing constraints
    /// (part size and copy chunk size both between 5 MiB and 5 GiB) are
    /// violated, or when the underlying HTTP client fails to initialise.
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        let constraints = [
            (
                config.multipart_part_size >= ByteSize::mib(5),
                "Multipart part size must be at least 5MiB",
            ),
            (
                config.multipart_part_size <= ByteSize::gib(5),
                "Multipart part size must be at most 5GiB",
            ),
            (
                config.multipart_copy_chunk_size <= ByteSize::gib(5),
                "Multipart copy chunk size must be at most 5GiB",
            ),
            (
                config.multipart_copy_chunk_size >= ByteSize::mib(5),
                "Multipart copy chunk size must be at least 5MiB",
            ),
            (
                config.circuit_breaker_threshold > 0,
                "Circuit breaker threshold must be at least 1",
            ),
            (
                config.operation_timeout_secs > 0 && config.operation_attempt_timeout_secs > 0,
                "Operation timeouts must be at least 1 second",
            ),
        ];
        if let Some((_, message)) = constraints.iter().find(|(holds, _)| !holds) {
            return Err(Error::Configuration((*message).to_string()));
        }

        let s3_client = S3Client::new(config)
            .map_err(|e| Error::Configuration(format!("failed to initialize S3 client: {e}")))?;

        Ok(Self {
            s3_client,
            key_prefix: config.key_prefix.clone(),
            multipart_copy_threshold: config.multipart_copy_threshold.as_u64(),
            multipart_copy_chunk_size: config.multipart_copy_chunk_size.as_u64(),
            multipart_copy_jobs: config.multipart_copy_jobs,
            circuit_breaker: CircuitBreaker::new(
                config.circuit_breaker_threshold,
                config.circuit_breaker_cooldown_secs,
            ),
        })
    }

    /// `NotFound`, `PreconditionFailed` and `Rejected` count as success since
    /// the backend answered, so denied actions cannot cascade into an outage.
    async fn guarded<T>(&self, op: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
        self.circuit_breaker.check()?;
        let result = op.await;
        match &result {
            Ok(_) | Err(Error::PreconditionFailed | Error::NotFound(_) | Error::Rejected(_)) => {
                self.circuit_breaker.record_success();
            }
            Err(_) => self.circuit_breaker.record_failure(),
        }
        result
    }

    fn full_key(&self, path: &str) -> String {
        if self.key_prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.key_prefix, path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::mock_config;

    fn test_config(overrides: impl FnOnce(&mut BackendConfig)) -> BackendConfig {
        let mut config = mock_config("http://localhost:9000".to_owned());
        overrides(&mut config);
        config
    }

    /// A zero breaker threshold opens before the first failure, and a zero
    /// timeout expires every request: both make the client answer nothing.
    #[test]
    fn test_new_rejects_out_of_range_settings() {
        let overrides: [fn(&mut BackendConfig); 7] = [
            |c| c.multipart_part_size = ByteSize::mib(4),
            |c| c.multipart_part_size = ByteSize::gib(6),
            |c| c.multipart_copy_chunk_size = ByteSize::mib(4),
            |c| c.multipart_copy_chunk_size = ByteSize::gib(6),
            |c| c.circuit_breaker_threshold = 0,
            |c| c.operation_timeout_secs = 0,
            |c| c.operation_attempt_timeout_secs = 0,
        ];
        for (index, override_config) in overrides.into_iter().enumerate() {
            let result = Backend::new(&test_config(override_config));
            assert!(
                matches!(result, Err(Error::Configuration(_))),
                "override {index} must be rejected"
            );
        }
    }

    #[test]
    fn test_full_key_without_prefix() {
        let backend = Backend::new(&test_config(|_| {})).unwrap();
        assert_eq!(backend.full_key("test/file.txt"), "test/file.txt");
    }

    #[test]
    fn test_full_key_with_prefix() {
        let backend = Backend::new(&test_config(|c| c.key_prefix = "prefix".to_string())).unwrap();
        assert_eq!(backend.full_key("test/file.txt"), "prefix/test/file.txt");
    }
}
