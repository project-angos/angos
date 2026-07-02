pub mod action;
pub mod check;
pub mod command;
pub mod context;
mod error;
pub mod executor;
pub mod node;
mod raw_sweep;
pub mod report;
pub mod scrub_lock;
pub mod setup;
pub mod sweep_sink;

pub use command::{Command, Options};
pub use error::Error;

#[cfg(test)]
pub mod test_support {
    //! Shared test fixtures for the `scrub`/`policy`/`replication` command tests:
    //! the minimal fs-backed `Configuration`, the replication-downstream config
    //! and its wiremock fixture, and an in-memory `tracing` capture.

    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    use angos_storage::{
        BoxedReader, ByteStream, ChildrenPage, Error as StorageError, MemoryObjectStore,
        MultipartUploadPage, ObjectMeta, ObjectStore, Page,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{Duration as ChronoDuration, Utc};
    use tracing_subscriber::fmt::MakeWriter;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{
        configuration::Configuration,
        oci::Digest,
        registry::{
            DOCKER_CONTENT_DIGEST, metadata_store::MetadataStore, test_utils::test_s3_endpoint,
        },
    };

    /// A minimal valid fs-backed `Configuration` rooted at `path`, with its
    /// `retention_policy.rules` set to the caller-supplied `rules` literal (the
    /// raw contents of the TOML array, e.g. `""` for none or
    /// `"\"image.tag == 'keep-me'\""` for a dropping rule).
    pub fn minimal_fs_config(path: &str, rules: &str) -> Configuration {
        let config_content = format!(
            r#"
            [blob_store.fs]
            root_dir = "{path}"

            [metadata_store.fs]
            root_dir = "{path}"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false

            [global.retention_policy]
            rules = [{rules}]
            "#
        );
        toml::from_str(&config_content).unwrap()
    }

    /// A `Configuration` whose blob and metadata stores both point at the shared
    /// S3 throwaway backend under a unique `key_prefix`, with the metadata
    /// store's `lock_strategy = "s3"` (a real cross-process maintenance lock)
    /// and `retention_policy.rules` set to the caller-supplied `rules` literal.
    /// Returns the config and the prefix (for cleanup).
    pub fn s3_lock_config(rules: &str) -> (Configuration, String) {
        let key_prefix = format!("test-scrublock-{}", uuid::Uuid::new_v4());
        let endpoint = test_s3_endpoint();
        let config_content = format!(
            r#"
            [blob_store.s3]
            access_key_id = "root"
            secret_key = "roottoor"
            endpoint = "{endpoint}"
            bucket = "registry"
            region = "region"
            key_prefix = "{key_prefix}"

            [metadata_store.s3]
            access_key_id = "root"
            secret_key = "roottoor"
            endpoint = "{endpoint}"
            bucket = "registry"
            region = "region"
            key_prefix = "{key_prefix}"

            [metadata_store.s3.lock_strategy.s3]

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false

            [global.retention_policy]
            rules = [{rules}]
            "#
        );
        let config = toml::from_str(&config_content).expect("s3 lock config parses");
        (config, key_prefix)
    }

    /// Delete every object under the config's `key_prefix` on the shared S3
    /// bucket, the same cleanup `S3RegistryTestCase` performs.
    pub async fn cleanup_s3_prefix(metadata_store: &MetadataStore) {
        if let Err(error) = metadata_store.store().delete_prefix("").await {
            println!("Warning: failed to clean up s3 lock test prefix: {error:?}");
        }
    }

    /// Like [`s3_lock_config`] but with the `nginx` repository
    /// event+reconcile-replicating to `downstream_uri`. When `prune` is true the
    /// downstream is marked `prune = true`.
    pub fn s3_config_with_replication(
        downstream_uri: &str,
        prune: bool,
    ) -> (Configuration, String) {
        let prune_line = if prune { "prune = true" } else { "" };
        let key_prefix = format!("test-scrublock-{}", uuid::Uuid::new_v4());
        let endpoint = test_s3_endpoint();
        let config_content = format!(
            r#"
            [blob_store.s3]
            access_key_id = "root"
            secret_key = "roottoor"
            endpoint = "{endpoint}"
            bucket = "registry"
            region = "region"
            key_prefix = "{key_prefix}"

            [metadata_store.s3]
            access_key_id = "root"
            secret_key = "roottoor"
            endpoint = "{endpoint}"
            bucket = "registry"
            region = "region"
            key_prefix = "{key_prefix}"

            [metadata_store.s3.lock_strategy.s3]

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false

            [global.retention_policy]
            rules = []

            [repository.nginx]

            [[repository.nginx.downstream]]
            name = "eu-region"
            url = "{downstream_uri}"
            mode = "event+reconcile"
            {prune_line}
            "#
        );
        let config = toml::from_str(&config_content).expect("s3 replication config parses");
        (config, key_prefix)
    }

    /// A config whose `nginx` repository event+reconcile-replicates to the
    /// downstream at `downstream_uri`. When `prune` is true the downstream is
    /// marked `prune = true`, so the reconcile also deletes downstream-only tags
    /// (one-way mirror).
    pub fn config_with_replication(path: &str, downstream_uri: &str, prune: bool) -> Configuration {
        let prune_line = if prune { "prune = true" } else { "" };
        let config_content = format!(
            r#"
            [blob_store.fs]
            root_dir = "{path}"

            [metadata_store.fs]
            root_dir = "{path}"

            [cache.memory]

            [server]
            bind_address = "0.0.0.0"
            port = 8000

            [global]
            update_pull_time = false

            [global.retention_policy]
            rules = []

            [repository.nginx]

            [[repository.nginx.downstream]]
            name = "eu-region"
            url = "{downstream_uri}"
            mode = "event+reconcile"
            {prune_line}
            "#
        );
        toml::from_str(&config_content).unwrap()
    }

    /// Mounts a downstream missing tag `v1` and both blobs, expecting the full
    /// blob-upload sequence and exactly one tagged manifest PUT.
    pub async fn mount_out_of_sync_downstream(
        mock_server: &MockServer,
        manifest_digest: &Digest,
        config_digest: &Digest,
        layer_digest: &Digest,
    ) {
        Mock::given(method("HEAD"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1..)
            .mount(mock_server)
            .await;
        for blob in [config_digest, layer_digest] {
            Mock::given(method("HEAD"))
                .and(path(format!("/v2/nginx/blobs/{blob}")))
                .respond_with(ResponseTemplate::new(404))
                .expect(1)
                .mount(mock_server)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/v2/nginx/blobs/uploads/"))
            .respond_with(
                ResponseTemplate::new(202).insert_header("Location", "/v2/nginx/blobs/uploads/s1"),
            )
            .expect(2)
            .mount(mock_server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/v2/nginx/blobs/uploads/s1"))
            .respond_with(
                ResponseTemplate::new(202).insert_header("Location", "/v2/nginx/blobs/uploads/s1"),
            )
            .expect(2)
            .mount(mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/v2/nginx/blobs/uploads/s1"))
            .respond_with(ResponseTemplate::new(201))
            .expect(2)
            .mount(mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/v2/nginx/manifests/v1"))
            .respond_with(
                ResponseTemplate::new(201)
                    .insert_header(DOCKER_CONTENT_DIGEST, manifest_digest.to_string().as_str()),
            )
            .expect(1)
            .mount(mock_server)
            .await;
    }

    /// In-memory `tracing` capture so a test can assert on emitted log text. Pairs
    /// with a subscriber built via
    /// `tracing_subscriber::fmt().with_writer(capture.clone())` run under
    /// `tracing::subscriber::with_default(...)`.
    #[derive(Clone, Default)]
    pub struct LogCapture(Arc<Mutex<Vec<u8>>>);

    pub struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl LogCapture {
        pub fn contents(&self) -> String {
            let bytes = self.0.lock().unwrap().clone();
            String::from_utf8(bytes).unwrap()
        }
    }

    impl Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for LogCapture {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(Arc::clone(&self.0))
        }
    }

    /// An `ObjectStore` wrapper that backdates every `head`'s `last_modified` over
    /// an in-memory store. `MemoryObjectStore` returns `None`, so this lets a test
    /// plant a fresh orphan and have it read as aged past the `BodyJanitor`'s
    /// default 1h age. Every other call delegates unchanged.
    #[derive(Debug)]
    pub struct AgedObjectStore {
        inner: Arc<MemoryObjectStore>,
        backdate: ChronoDuration,
    }

    impl AgedObjectStore {
        pub fn new(inner: Arc<MemoryObjectStore>, backdate: ChronoDuration) -> Self {
            Self { inner, backdate }
        }
    }

    #[async_trait]
    impl ObjectStore for AgedObjectStore {
        async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.inner.get(key).await
        }
        async fn get_stream(
            &self,
            key: &str,
            offset: Option<u64>,
        ) -> Result<(BoxedReader, u64), StorageError> {
            self.inner.get_stream(key, offset).await
        }
        async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
            self.inner.put(key, data).await
        }
        async fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key).await
        }
        async fn delete_prefix(&self, prefix: &str) -> Result<(), StorageError> {
            self.inner.delete_prefix(prefix).await
        }
        async fn head(&self, key: &str) -> Result<ObjectMeta, StorageError> {
            let mut m = self.inner.head(key).await?;
            m.last_modified = Some(Utc::now() - self.backdate);
            Ok(m)
        }
        async fn list(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
        ) -> Result<Page<String>, StorageError> {
            self.inner.list(prefix, n, token).await
        }
        async fn list_children(
            &self,
            prefix: &str,
            n: u16,
            token: Option<String>,
            start_after: Option<String>,
        ) -> Result<ChildrenPage, StorageError> {
            self.inner
                .list_children(prefix, n, token, start_after)
                .await
        }
        async fn copy(&self, source: &str, destination: &str) -> Result<(), StorageError> {
            self.inner.copy(source, destination).await
        }
        async fn create_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.create_upload(key).await
        }
        async fn write_upload(
            &self,
            key: &str,
            body: ByteStream,
            len: Option<u64>,
        ) -> Result<u64, StorageError> {
            self.inner.write_upload(key, body, len).await
        }
        async fn complete_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.complete_upload(key).await
        }
        async fn abort_upload(&self, key: &str) -> Result<(), StorageError> {
            self.inner.abort_upload(key).await
        }
        async fn list_multipart_uploads(
            &self,
            key_marker: Option<&str>,
            upload_id_marker: Option<&str>,
        ) -> Result<MultipartUploadPage, StorageError> {
            self.inner
                .list_multipart_uploads(key_marker, upload_id_marker)
                .await
        }
    }
}
