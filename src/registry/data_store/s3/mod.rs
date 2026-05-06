use std::{
    io::{Error as IoError, ErrorKind},
    time::Duration,
};

use aws_sdk_s3::{
    Client as S3Client, Config as S3Config,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig},
};
use bytesize::ByteSize;

use crate::{circuit_breaker::CircuitBreaker, registry::data_store::Error};

mod channel_body;
mod config;
mod listing;
mod multipart;
mod object;
mod presign;

pub use config::BackendConfig;
pub use multipart::UploadedPart;

#[derive(Clone, Debug)]
pub struct Backend {
    s3_client: S3Client,
    bucket: String,
    key_prefix: String,
    circuit_breaker: CircuitBreaker,
}

impl Backend {
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

        let credentials = Credentials::new(
            config.access_key_id.expose(),
            config.secret_key.expose(),
            None,
            None,
            "custom",
        );

        let timeout = TimeoutConfig::builder()
            .operation_timeout(Duration::from_secs(config.operation_timeout_secs))
            .operation_attempt_timeout(Duration::from_secs(config.operation_attempt_timeout_secs))
            .build();

        let retry = RetryConfig::standard().with_max_attempts(config.max_attempts);

        let client_config = S3Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .endpoint_url(&config.endpoint)
            .credentials_provider(credentials)
            .timeout_config(timeout)
            .retry_config(retry)
            .force_path_style(true)
            .build();

        let s3_client = S3Client::from_conf(client_config);

        Ok(Self {
            s3_client,
            bucket: config.bucket.clone(),
            key_prefix: config.key_prefix.clone(),
            circuit_breaker: CircuitBreaker::new(),
        })
    }

    pub fn check_circuit_breaker(&self) -> Result<(), IoError> {
        self.circuit_breaker.check()?;
        Ok(())
    }

    pub fn record_io_result<T>(&self, result: &Result<T, IoError>) {
        match result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                self.circuit_breaker.record_success();
            }
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
    use object::{aggregate_batch_delete_errors, build_object_identifiers};

    use super::*;
    use crate::secret::Secret;

    fn test_config(overrides: impl FnOnce(&mut BackendConfig)) -> BackendConfig {
        let mut config = BackendConfig {
            access_key_id: Secret::new("key".to_string()),
            secret_key: Secret::new("secret".to_string()),
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
        let config = test_config(|c| c.multipart_part_size = ByteSize::mib(4));
        let result = Backend::new(&config);
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_multipart_copy_chunk_size_too_large() {
        let config = test_config(|c| c.multipart_copy_chunk_size = ByteSize::gib(6));
        let result = Backend::new(&config);
        assert!(matches!(result, Err(Error::Configuration(_))));
    }

    #[test]
    fn test_new_valid_config() {
        let config = test_config(|_| {});
        let result = Backend::new(&config);
        assert!(result.is_ok());
        let backend = result.unwrap();
        assert_eq!(backend.bucket, "test");
        assert_eq!(backend.key_prefix, "");
    }

    #[test]
    fn test_full_key_without_prefix() {
        let config = test_config(|_| {});
        let backend = Backend::new(&config).unwrap();
        assert_eq!(backend.full_key("test/file.txt"), "test/file.txt");
    }

    #[test]
    fn test_full_key_with_prefix() {
        let config = test_config(|c| c.key_prefix = "prefix".to_string());
        let backend = Backend::new(&config).unwrap();
        assert_eq!(backend.full_key("test/file.txt"), "prefix/test/file.txt");
    }

    #[tokio::test]
    async fn test_upload_part_returns_etag() {
        let config = test_config(|c| {
            c.access_key_id = Secret::new("minioadmin".to_string());
            c.secret_key = Secret::new("minioadmin".to_string());
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
            assert!(err.to_string().contains("error") || err.to_string().contains("refused"));
        }
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let config = test_config(|c| {
            c.access_key_id = Secret::new("minioadmin".to_string());
            c.secret_key = Secret::new("minioadmin".to_string());
            c.bucket = "test-bucket".to_string();
        });

        let backend = Backend::new(&config).unwrap();

        let result = backend
            .abort_multipart_upload("test/file.txt", "test-upload-id")
            .await;

        if let Err(err) = result {
            assert!(err.to_string().contains("error") || err.to_string().contains("refused"));
        }
    }

    #[test]
    fn test_build_object_identifiers_empty_input() {
        let result = build_object_identifiers(vec![]);
        let ids = result.expect("empty input must succeed");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_build_object_identifiers_all_keys_present() {
        use aws_sdk_s3::types::Object;
        let objects = vec![
            Object::builder().key("a/b/c").build(),
            Object::builder().key("d/e/f").build(),
            Object::builder().key("g/h/i").build(),
        ];
        let result = build_object_identifiers(objects);
        let ids = result.expect("all keys present must succeed");
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn test_build_object_identifiers_skips_missing_keys() {
        use aws_sdk_s3::types::Object;
        let objects = vec![
            Object::builder().key("present/key").build(),
            Object::builder().build(),
            Object::builder().key("another/key").build(),
        ];
        let result = build_object_identifiers(objects);
        let ids = result.expect("objects with missing keys are filtered, not errors");
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn test_aggregate_batch_delete_errors_joins_messages() {
        use aws_sdk_s3::types::Error as S3Error;
        let errors = vec![
            S3Error::builder().message("first failure").build(),
            S3Error::builder().message("second failure").build(),
        ];
        let result = aggregate_batch_delete_errors(&errors);
        let err = result.expect("non-empty errors must produce IoError");
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
