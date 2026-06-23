pub mod action;
pub mod check;
pub mod command;
pub mod context;
mod error;
pub mod executor;
pub mod node;
pub mod report;
pub mod scheduler;
pub mod setup;

pub use command::{Command, Options};
pub use error::Error;

#[cfg(test)]
pub(crate) mod test_support {
    //! Shared test fixtures for the `scrub`/`policy`/`replication` command tests:
    //! the minimal fs-backed `Configuration`, the replication-downstream config
    //! and its wiremock fixture, and an in-memory `tracing` capture.

    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    use tracing_subscriber::fmt::MakeWriter;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{configuration::Configuration, oci::Digest, registry::DOCKER_CONTENT_DIGEST};

    /// A minimal valid fs-backed `Configuration` rooted at `path`, with its
    /// `retention_policy.rules` set to the caller-supplied `rules` literal (the
    /// raw contents of the TOML array, e.g. `""` for none or
    /// `"\"image.tag == 'keep-me'\""` for a dropping rule).
    pub(crate) fn minimal_fs_config(path: &str, rules: &str) -> Configuration {
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

    /// A config whose `nginx` repository event+reconcile-replicates to the
    /// downstream at `downstream_uri`. When `prune` is true the downstream is
    /// marked `prune = true`, so the reconcile also deletes downstream-only tags
    /// (one-way mirror).
    pub(crate) fn config_with_replication(
        path: &str,
        downstream_uri: &str,
        prune: bool,
    ) -> Configuration {
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
    pub(crate) async fn mount_out_of_sync_downstream(
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
    pub(crate) struct LogCapture(Arc<Mutex<Vec<u8>>>);

    pub(crate) struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl LogCapture {
        pub(crate) fn contents(&self) -> String {
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
}
