//! Blob-store configuration.
//!
//! [`BlobStoreConfig`] is the TOML-facing enum operators write under
//! `[blob_store.fs]` or `[blob_store.s3]`. It selects which storage backend to
//! instantiate, but the resulting [`BlobStore`] is the same unified type
//! regardless: each arm wires the object store plus, on S3, the presign
//! backend. The blob store holds no transaction executor: blob-lifecycle
//! serialisation lives on the metadata store's `blob-data:{digest}` lock.

use std::sync::Arc;

use bytesize::ByteSize;
use serde::Deserialize;

use angos_s3_client::{Backend as S3HttpBackend, BackendConfig as S3TransportConfig};
use angos_storage::{
    ObjectStore, PresignedStore, fs::Backend as StorageFsBackend, s3::Backend as StorageS3Backend,
};

use crate::registry::{
    blob_store::{BlobStore, Error},
    s3_connection::S3ConnectionConfig,
};

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct FsBackendConfig {
    pub root_dir: String,
    #[serde(default)]
    pub sync_to_disk: bool,
}

/// S3-backed blob store. Connection fields are required (matching the
/// documented schema); only `key_prefix` may be omitted. Transport fields
/// default through [`TransportFields`]'s struct-level `#[serde(default)]`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct S3BackendConfig {
    #[serde(flatten)]
    pub connection: S3ConnectionConfig,
    #[serde(flatten)]
    pub transport: TransportFields,
}

/// Blob-store-specific transport knobs. Mirrors the non-connection fields
/// of [`S3TransportConfig`] so the blob-store config can use
/// `Secret`-wrapped credentials via [`S3ConnectionConfig`] while still
/// exposing the same flat TOML keys to operators.
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
}

impl Default for TransportFields {
    fn default() -> Self {
        let t = S3TransportConfig::default();
        Self {
            multipart_copy_threshold: t.multipart_copy_threshold,
            multipart_copy_chunk_size: t.multipart_copy_chunk_size,
            multipart_copy_jobs: t.multipart_copy_jobs,
            multipart_part_size: t.multipart_part_size,
            multipart_uniform_parts: t.multipart_uniform_parts,
            operation_timeout_secs: t.operation_timeout_secs,
            operation_attempt_timeout_secs: t.operation_attempt_timeout_secs,
            max_attempts: t.max_attempts,
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

impl Default for BlobStoreConfig {
    fn default() -> Self {
        BlobStoreConfig::FS(FsBackendConfig::default())
    }
}

impl BlobStoreConfig {
    /// Build the unified [`BlobStore`].
    ///
    /// FS wires only the object store (the backend prunes its own empty
    /// directories on delete). S3 additionally wires the presign backend for
    /// download URLs. Neither wires a transaction executor: the blob store is
    /// pure storage and coordinates through the metadata store's lock.
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
                let transport = S3TransportConfig {
                    multipart_copy_threshold: config.transport.multipart_copy_threshold,
                    multipart_copy_chunk_size: config.transport.multipart_copy_chunk_size,
                    multipart_copy_jobs: config.transport.multipart_copy_jobs,
                    multipart_part_size: config.transport.multipart_part_size,
                    multipart_uniform_parts: config.transport.multipart_uniform_parts,
                    operation_timeout_secs: config.transport.operation_timeout_secs,
                    operation_attempt_timeout_secs: config.transport.operation_attempt_timeout_secs,
                    max_attempts: config.transport.max_attempts,
                    ..config.connection.to_client_config()
                };
                let http = S3HttpBackend::new(&transport)
                    .map_err(|e| Error::StorageBackend(e.to_string()))?;
                let backend = Arc::new(
                    StorageS3Backend::builder(Arc::new(http))
                        .part_size(config.transport.multipart_part_size.as_u64())
                        .uniform_parts(config.transport.multipart_uniform_parts)
                        .build(),
                );
                let object: Arc<dyn ObjectStore> = backend.clone();
                let presign: Arc<dyn PresignedStore> = backend;
                Ok(BlobStore::new(object, Some(presign)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::secret::Secret;

    #[tokio::test]
    async fn fs_backend_builds() {
        let temp_dir = TempDir::new().unwrap();
        let config = BlobStoreConfig::FS(FsBackendConfig {
            root_dir: temp_dir.path().to_string_lossy().to_string(),
            sync_to_disk: false,
        });
        let backend = config.build_backend().unwrap();
        assert!(!backend.supports_presign());
    }

    #[test]
    fn s3_backend_builds_with_presign() {
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
        assert!(backend.supports_presign());
    }

    /// `[blob_store.s3]` round-trip: flat TOML deserialises into both the
    /// embedded `S3ConnectionConfig` and the `TransportFields` knobs.
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

    /// Regression: connection fields are required.
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
