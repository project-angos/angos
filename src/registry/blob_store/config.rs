//! Blob-store configuration: the TOML-facing [`BlobStoreConfig`] operators
//! write under `[blob_store.fs]` or `[blob_store.s3]`, and the wiring of the
//! storage handles it selects.

use std::{path::PathBuf, sync::Arc, time::Duration};

use bytesize::ByteSize;
use serde::Deserialize;

use angos_s3_client::{
    Backend as S3HttpBackend, BackendConfig as S3TransportConfig, Error as S3ClientError,
};
use angos_storage::{
    ObjectStore, PresignedStore,
    fs::Backend as StorageFsBackend,
    s3::{Backend as StorageS3Backend, DEFAULT_RANGE_CONCURRENCY},
};

use crate::registry::{
    Error,
    blob_store::{BlobStore, DEFAULT_PRESIGN_TTL_SECS},
    s3_connection::S3ConnectionConfig,
};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct FsBackendConfig {
    pub root_dir: PathBuf,
    #[serde(default)]
    pub sync_to_disk: bool,
}

/// An S3-backed store, for blobs or metadata. Connection fields are required
/// apart from `key_prefix`; transport fields all default.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct S3BackendConfig {
    #[serde(flatten)]
    pub connection: S3ConnectionConfig,
    #[serde(flatten)]
    pub transport: TransportFields,
}

impl S3BackendConfig {
    /// The object store these settings describe, connection and transport
    /// alike; the one place either store's S3 backend is built.
    pub fn build(&self) -> Result<StorageS3Backend, S3ClientError> {
        let transport = S3TransportConfig {
            multipart_copy_threshold: self.transport.multipart_copy_threshold,
            multipart_copy_chunk_size: self.transport.multipart_copy_chunk_size,
            multipart_copy_jobs: self.transport.multipart_copy_jobs,
            multipart_part_size: self.transport.multipart_part_size,
            operation_timeout_secs: self.transport.operation_timeout_secs,
            operation_attempt_timeout_secs: self.transport.operation_attempt_timeout_secs,
            max_attempts: self.transport.max_attempts,
            circuit_breaker_threshold: self.transport.circuit_breaker_threshold,
            circuit_breaker_cooldown_secs: self.transport.circuit_breaker_cooldown_secs,
            ..self.connection.to_client_config()
        };
        let http = S3HttpBackend::new(&transport)?;
        Ok(StorageS3Backend::builder(Arc::new(http))
            .part_size(self.transport.multipart_part_size.as_u64())
            .uniform_parts(self.transport.multipart_uniform_parts)
            .range_concurrency(self.transport.children_scan_concurrency)
            .build())
    }
}

/// Blob-store transport knobs, mirroring the non-connection fields of
/// [`S3TransportConfig`] so credentials can stay `Secret`-wrapped in
/// [`S3ConnectionConfig`] while operators keep the same flat TOML keys.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(default)]
pub struct TransportFields {
    pub multipart_copy_threshold: ByteSize,
    pub multipart_copy_chunk_size: ByteSize,
    pub multipart_copy_jobs: usize,
    pub multipart_part_size: ByteSize,
    pub multipart_uniform_parts: bool,
    pub operation_timeout_secs: u64,
    pub operation_attempt_timeout_secs: u64,
    pub max_attempts: u32,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_cooldown_secs: u64,
    /// Max concurrent range chains a truncated children/flat scan fans out to.
    pub children_scan_concurrency: usize,
    /// Lifetime, in seconds, of a generated presigned download URL.
    pub presign_ttl_secs: u64,
}

impl Default for TransportFields {
    fn default() -> Self {
        let t = S3TransportConfig::default();
        Self {
            multipart_copy_threshold: t.multipart_copy_threshold,
            multipart_copy_chunk_size: t.multipart_copy_chunk_size,
            multipart_copy_jobs: t.multipart_copy_jobs,
            multipart_part_size: t.multipart_part_size,
            multipart_uniform_parts: false,
            operation_timeout_secs: t.operation_timeout_secs,
            operation_attempt_timeout_secs: t.operation_attempt_timeout_secs,
            max_attempts: t.max_attempts,
            circuit_breaker_threshold: t.circuit_breaker_threshold,
            circuit_breaker_cooldown_secs: t.circuit_breaker_cooldown_secs,
            children_scan_concurrency: DEFAULT_RANGE_CONCURRENCY,
            presign_ttl_secs: DEFAULT_PRESIGN_TTL_SECS,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum BlobStoreConfig {
    #[serde(rename = "fs")]
    FS(FsBackendConfig),
    #[serde(rename = "s3")]
    S3(S3BackendConfig),
}

impl BlobStoreConfig {
    /// Build the [`BlobStore`]: FS wires only the object store, S3 also wires
    /// the presign backend for download URLs.
    pub fn build_backend(&self) -> Result<BlobStore, Error> {
        match self {
            BlobStoreConfig::FS(config) => {
                let object: Arc<dyn ObjectStore> = Arc::new(
                    StorageFsBackend::builder(&config.root_dir)
                        .sync_to_disk(config.sync_to_disk)
                        .build(),
                );
                Ok(BlobStore::new(object, None))
            }
            BlobStoreConfig::S3(config) => {
                let backend = Arc::new(config.build().map_err(|e| Error::Internal(e.to_string()))?);
                let object: Arc<dyn ObjectStore> = backend.clone();
                let presign: Arc<dyn PresignedStore> = backend;
                let ttl = Duration::from_secs(config.transport.presign_ttl_secs);
                Ok(BlobStore::new(object, Some((presign, ttl))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use angos_oci::Digest;
    use angos_secret::Secret;

    use super::*;

    #[tokio::test]
    async fn fs_backend_builds() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlobStoreConfig::FS(FsBackendConfig {
            root_dir: temp_dir.path().to_path_buf(),
            sync_to_disk: false,
        });
        let backend = config.build_backend().unwrap();
        assert_eq!(
            backend
                .presigned_url(&Digest::sha256_of_bytes(b"blob"), None)
                .await
                .unwrap(),
            None,
            "a filesystem store signs no URLs"
        );
    }

    #[tokio::test]
    async fn s3_backend_builds_with_presign() {
        let config = BlobStoreConfig::S3(S3BackendConfig {
            connection: S3ConnectionConfig {
                access_key_id: Secret::new("root".to_string()),
                secret_key: Secret::new("roottoor".to_string()),
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                key_prefix: String::new(),
            },
            ..S3BackendConfig::default()
        });
        let backend = config.build_backend().unwrap();
        assert!(
            backend
                .presigned_url(&Digest::sha256_of_bytes(b"blob"), None)
                .await
                .unwrap()
                .is_some(),
            "an S3 store signs a download URL"
        );
    }

    /// Flat TOML must deserialise into both the embedded `S3ConnectionConfig`
    /// and the `TransportFields` knobs.
    #[test]
    fn s3_backend_config_toml_round_trip() {
        let toml = r#"
            access_key_id             = "blob-key"
            secret_key                = "blob-secret"
            endpoint                  = "https://blob.s3.example.com"
            bucket                    = "blob-bucket"
            region                    = "us-west-2"
            key_prefix                = "_blobs"
            multipart_part_size       = "50 MiB"
            multipart_uniform_parts   = true
            multipart_copy_threshold  = "5 GiB"
            multipart_copy_chunk_size = "100 MiB"
            multipart_copy_jobs       = 8
        "#;

        let cfg: S3BackendConfig = toml::from_str(toml).expect("deserialize");
        assert_eq!(cfg.connection.access_key_id.expose(), "blob-key");
        assert_eq!(cfg.connection.secret_key.expose(), "blob-secret");
        assert_eq!(cfg.connection.endpoint, "https://blob.s3.example.com");
        assert_eq!(cfg.connection.bucket, "blob-bucket");
        assert_eq!(cfg.connection.region, "us-west-2");
        assert_eq!(cfg.connection.key_prefix, "_blobs");
        assert!(cfg.transport.multipart_uniform_parts);
        assert_eq!(cfg.transport.multipart_copy_jobs, 8);
    }

    /// Connection fields are required, not defaulted.
    #[test]
    fn s3_backend_config_requires_region() {
        let toml = r#"
            access_key_id = "k"
            secret_key    = "s"
            endpoint      = "http://localhost:9000"
            bucket        = "b"
        "#;
        let err = toml::from_str::<S3BackendConfig>(toml).expect_err("region must be required");
        assert!(
            err.to_string().contains("region"),
            "error should mention the missing `region` field, got: {err}"
        );
    }
}
