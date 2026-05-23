//! S3-compatible storage client.
//!
//! This module is the single seam between the rest of the crate and "the
//! wire": all `reqwest`, `hmac`, `sha2`, `quick_xml` and other HTTP/S3
//! implementation details live behind `client`, `xml` and `ops`. Anything
//! exported from this module is intentionally framework-neutral (`Bytes`,
//! `io::Error`, the storage-level [`Error`] and a handful of plain-data record
//! types).

use std::io;

use bytesize::ByteSize;

use crate::circuit_breaker::CircuitBreaker;

mod circuit_breaker;
mod client;
mod config;
mod error;
mod ops;
mod xml;

pub use crate::config::BackendConfig;
pub use crate::error::Error;
pub use crate::ops::UploadedPart;

#[derive(Clone, Debug)]
pub struct Backend {
    pub s3_client: client::S3Client,
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
    /// (part size ≥ 5 MiB, copy chunk size between 5 MiB and 5 GiB) are
    /// violated, or when the underlying HTTP client fails to initialise.
    pub fn new(config: &BackendConfig) -> Result<Self, Error> {
        if config.multipart_part_size < ByteSize::mib(5) {
            return Err(Error::Configuration(
                "Multipart part size must be at least 5MiB".to_string(),
            ));
        }
        if config.multipart_copy_chunk_size > ByteSize::gib(5) {
            return Err(Error::Configuration(
                "Multipart copy chunk size must be at most 5GiB".to_string(),
            ));
        }
        if config.multipart_copy_chunk_size < ByteSize::mib(5) {
            return Err(Error::Configuration(
                "Multipart copy chunk size must be at least 5MiB".to_string(),
            ));
        }

        let s3_client = client::S3Client::new(config)
            .map_err(|e| Error::Configuration(format!("failed to initialize S3 client: {e}")))?;

        Ok(Self {
            s3_client,
            encoded_bucket: client::encode_bucket(&config.bucket),
            key_prefix: config.key_prefix.clone(),
            multipart_copy_threshold: config.multipart_copy_threshold.as_u64(),
            multipart_copy_chunk_size: config.multipart_copy_chunk_size.as_u64(),
            multipart_copy_jobs: config.multipart_copy_jobs,
            circuit_breaker: CircuitBreaker::new(),
        })
    }

    /// Short-circuit if the breaker is currently open.
    ///
    /// # Errors
    /// Returns an [`io::Error`] with [`io::ErrorKind::Other`] when the
    /// circuit breaker has tripped on repeated upstream failures.
    pub fn check_circuit_breaker(&self) -> Result<(), io::Error> {
        self.circuit_breaker.check()?;
        Ok(())
    }

    pub fn record_io_result<T>(&self, result: &Result<T, io::Error>) {
        match result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => self.circuit_breaker.record_success(),
            Err(_) => self.circuit_breaker.record_failure(),
        }
    }

    pub fn record_data_result<T>(&self, result: &Result<T, Error>) {
        match result {
            Ok(_) | Err(Error::PreconditionFailed | Error::NotFound(_)) => {
                self.circuit_breaker.record_success();
            }
            Err(_) => self.circuit_breaker.record_failure(),
        }
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
    use bytes::Bytes;

    use super::{ops::aggregate_batch_delete_errors, *};

    fn test_config(overrides: impl FnOnce(&mut BackendConfig)) -> BackendConfig {
        let mut config = BackendConfig {
            access_key_id: "key".to_string(),
            secret_key: "secret".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            bucket: "test".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };
        overrides(&mut config);
        config
    }

    #[test]
    fn test_default_values() {
        let config = BackendConfig::default();
        assert_eq!(config.multipart_copy_threshold, ByteSize::gb(5));
        assert_eq!(config.multipart_copy_chunk_size, ByteSize::mb(100));
        assert_eq!(config.multipart_copy_jobs, 4);
        assert_eq!(config.multipart_part_size, ByteSize::mib(50));
        assert_eq!(config.operation_timeout_secs, 900);
        assert_eq!(config.operation_attempt_timeout_secs, 300);
        assert_eq!(config.max_attempts, 3);
    }

    #[test]
    fn test_new_multipart_part_size_too_small() {
        let result = Backend::new(&test_config(|c| c.multipart_part_size = ByteSize::mib(4)));
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
    fn test_new_valid_config() {
        let backend = Backend::new(&test_config(|_| {})).unwrap();
        assert_eq!(backend.key_prefix, "");
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

    #[tokio::test]
    async fn test_upload_part_returns_etag() {
        let config = test_config(|c| {
            c.access_key_id = "minioadmin".to_string();
            c.secret_key = "minioadmin".to_string();
            c.bucket = "test-bucket".to_string();
        });
        let backend = Backend::new(&config).unwrap();
        let result = backend
            .upload_part(
                "test/file.txt",
                "test-upload-id",
                1,
                Bytes::from("test data"),
            )
            .await;
        if let Err(err) = result {
            assert!(!err.to_string().is_empty());
        }
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let config = test_config(|c| {
            c.access_key_id = "minioadmin".to_string();
            c.secret_key = "minioadmin".to_string();
            c.bucket = "test-bucket".to_string();
        });
        let backend = Backend::new(&config).unwrap();
        let result = backend
            .abort_multipart_upload("test/file.txt", "test-upload-id")
            .await;
        if let Err(err) = result {
            assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn test_aggregate_batch_delete_errors_joins_messages() {
        let errors = vec!["first failure".to_string(), "second failure".to_string()];
        let err =
            aggregate_batch_delete_errors(&errors).expect("non-empty errors must produce IoError");
        let msg = err.to_string();
        assert!(msg.contains("batch delete errors:"), "got: {msg}");
        assert!(msg.contains("first failure"), "got: {msg}");
        assert!(msg.contains("second failure"), "got: {msg}");
        assert!(msg.contains("; "), "got: {msg}");
    }

    #[test]
    fn test_aggregate_batch_delete_errors_empty_returns_none() {
        assert!(aggregate_batch_delete_errors(&[]).is_none());
    }
}
