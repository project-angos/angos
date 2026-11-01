//! S3-compatible storage client.
//!
//! This module is the single seam between the rest of the crate and "the
//! wire": all `reqwest`, `hmac`, `sha2`, `quick_xml` and other HTTP/S3
//! implementation details live behind `client`, `xml` and `ops`. Anything
//! exported from this module is intentionally framework-neutral (`Bytes`,
//! `io::Error`, the storage-level [`Error`] and a handful of plain-data record
//! types).

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
    pub s3_client: S3Client,
    pub encoded_bucket: String,
    pub key_prefix: String,
    pub multipart_copy_threshold: u64,
    pub multipart_copy_chunk_size: u64,
    pub multipart_copy_jobs: usize,
    pub circuit_breaker: CircuitBreaker,
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
            encoded_bucket: client::encode_bucket(&config.bucket),
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

    /// Run `op` behind the circuit breaker: fail fast while the breaker is
    /// open, otherwise execute it and feed the outcome back. `NotFound`,
    /// `PreconditionFailed` and `Rejected` count as success since the backend
    /// answered; only a transport failure or a 5xx says it is unhealthy, so a
    /// single denied action cannot cascade into an outage.
    ///
    /// # Errors
    /// Returns [`Error::Io`] when the circuit breaker has tripped on repeated
    /// upstream failures; otherwise forwards `op`'s error.
    pub async fn guarded<T>(&self, op: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
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

    #[must_use]
    pub fn full_key(&self, path: &str) -> String {
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

    #[test]
    fn test_new_multipart_part_size_too_small() {
        let result = Backend::new(&test_config(|c| c.multipart_part_size = ByteSize::mib(4)));
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_multipart_part_size_too_large() {
        let result = Backend::new(&test_config(|c| c.multipart_part_size = ByteSize::gib(6)));
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    /// A zero threshold opens the breaker before the first failure, and a zero
    /// timeout expires every request: both make the client answer nothing.
    #[test]
    fn test_new_rejects_a_zero_circuit_breaker_threshold() {
        let result = Backend::new(&test_config(|c| c.circuit_breaker_threshold = 0));
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_rejects_zero_operation_timeouts() {
        let result = Backend::new(&test_config(|c| c.operation_timeout_secs = 0));
        assert!(matches!(result, Err(Error::Configuration(_))));
        let result = Backend::new(&test_config(|c| c.operation_attempt_timeout_secs = 0));
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_multipart_copy_chunk_size_too_large() {
        let result = Backend::new(&test_config(|c| {
            c.multipart_copy_chunk_size = ByteSize::gib(6);
        }));
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_multipart_copy_chunk_size_too_small() {
        let result = Backend::new(&test_config(|c| {
            c.multipart_copy_chunk_size = ByteSize::mib(4);
        }));
        assert!(matches!(result, Err(Error::Configuration(_))));
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
