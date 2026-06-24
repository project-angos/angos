use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

use serde::Deserialize;
use tokio::task;
use tracing::{instrument, warn};

pub use crate::registry_client::RegistryClientConfig;
use crate::{
    cache::Cache,
    configuration::RegexPattern,
    oci::{Digest, MediaType, Namespace, Reference},
    policy::{AccessPolicyConfig, RetentionPolicy, RetentionPolicyConfig, SystemClock},
    registry::{Error, blob_store::BoxedReader},
    registry_client::RegistryClient,
    replication::{ReplicationDownstream, ReplicationDownstreamConfig},
};

/// Fallback per-manifest blob-push concurrency when a downstream omits
/// `max_concurrent_pushes`.
const DEFAULT_MAX_CONCURRENT_PUSHES: usize = 4;

/// Split a registry URL (upstream or downstream) into the registry base (talked
/// to at its OCI `/v2/` root) and the path-derived namespace prefix.
/// `http://host:8000/team` yields (`http://host:8000`, `team`); a bare-host URL
/// yields an empty prefix. A path before `/v2/` is meaningless to an OCI
/// registry, so it is mapped into the namespace rather than the HTTP path.
fn split_registry_url(url: &str) -> (String, String) {
    let trimmed = url.trim_end_matches('/');
    if let Some(scheme_end) = trimmed.find("://") {
        let authority_start = scheme_end + 3;
        if let Some(path_offset) = trimmed[authority_start..].find('/') {
            let split_at = authority_start + path_offset;
            let base = trimmed[..split_at].to_string();
            let prefix = trimmed[split_at..].trim_matches('/').to_string();
            return (base, prefix);
        }
    }
    (trimmed.to_string(), String::new())
}

/// Build the pull-through upstream clients. A path on an upstream URL is the
/// namespace prefix the images live under upstream; angos talks to the registry
/// root and maps the path into the namespace. The repository name is stripped by
/// the `local_name` argument the pull path passes, so the mirror only prepends.
async fn build_upstreams(
    upstream_configs: &[RegistryClientConfig],
    cache: &Arc<Cache>,
    max_manifest_size_bytes: usize,
) -> Result<Vec<RegistryClient>, Error> {
    if upstream_configs.is_empty() {
        return Ok(Vec::new());
    }
    let upstream_configs = upstream_configs.to_vec();
    let cache = Arc::clone(cache);
    task::spawn_blocking(move || {
        let mut upstreams = Vec::new();
        for config in &upstream_configs {
            let (base_url, prefix) = split_registry_url(&config.url);
            let mut client_config = config.clone();
            client_config.url = base_url;
            let mut client = RegistryClient::from_config(
                &client_config,
                Arc::clone(&cache),
                max_manifest_size_bytes,
            )?;
            if !prefix.is_empty() {
                client = client.with_namespace_mirror(String::new(), prefix);
            }
            upstreams.push(client);
        }
        Ok::<_, Error>(upstreams)
    })
    .await
    .map_err(|e| Error::Internal(format!("Failed to initialize upstream clients: {e}")))?
}

/// Build the replication downstream clients after validating that every
/// downstream `name` is non-empty and unique (the name is the job routing key
/// and metrics label). A path on a downstream URL replaces this repository's
/// prefix (`repo/x` -> `prefix/x`); a bare-host URL mirrors the namespace
/// verbatim.
async fn build_downstreams(
    name: &str,
    downstream_configs: &[ReplicationDownstreamConfig],
    cache: &Arc<Cache>,
    max_manifest_size_bytes: usize,
) -> Result<Vec<ReplicationDownstream>, Error> {
    let mut seen_names = HashSet::new();
    for downstream in downstream_configs {
        if downstream.name.is_empty() {
            return Err(Error::Initialization(format!(
                "replication downstream in repository '{name}' has an empty name; \
                 a non-empty name is required (it is the job routing key and metrics label)"
            )));
        }
        if !seen_names.insert(downstream.name.as_str()) {
            return Err(Error::Initialization(format!(
                "repository '{name}' has duplicate replication downstream name '{}'; \
                 downstream names must be unique (they are the job routing key and metrics label)",
                downstream.name
            )));
        }
    }

    if downstream_configs.is_empty() {
        return Ok(Vec::new());
    }
    let downstream_configs = downstream_configs.to_vec();
    let cache = Arc::clone(cache);
    let repo_name = name.to_string();
    task::spawn_blocking(move || {
        let mut downstreams = Vec::new();
        for config in &downstream_configs {
            let (base_url, prefix) = split_registry_url(&config.client.url);
            let mut client_config = config.client.clone();
            client_config.url = base_url;
            let mut registry_client = RegistryClient::from_config(
                &client_config,
                Arc::clone(&cache),
                max_manifest_size_bytes,
            )?;
            if !prefix.is_empty() {
                registry_client = registry_client.with_namespace_mirror(repo_name.clone(), prefix);
            }
            downstreams.push(
                ReplicationDownstream::builder(
                    config.name.clone(),
                    Arc::new(registry_client),
                    config
                        .max_concurrent_pushes
                        .map_or(DEFAULT_MAX_CONCURRENT_PUSHES, NonZeroUsize::get),
                )
                .mode(config.mode)
                .namespace_filter(
                    config
                        .namespace_filter
                        .iter()
                        .cloned()
                        .map(RegexPattern::into_regex)
                        .collect(),
                )
                .prune(config.prune)
                .build(),
            );
        }
        Ok::<_, Error>(downstreams)
    })
    .await
    .map_err(|e| Error::Internal(format!("Failed to initialize downstream clients: {e}")))?
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub upstream: Vec<RegistryClientConfig>,
    #[serde(default)]
    pub downstream: Vec<ReplicationDownstreamConfig>,
    #[serde(default)]
    pub access_policy: AccessPolicyConfig,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
}

pub struct Repository {
    pub name: String,
    pub upstreams: Vec<RegistryClient>,
    pub replication: Vec<ReplicationDownstream>,
    pub retention_policy: RetentionPolicy,
    pub immutable_tags: bool,
    pub immutable_tags_exclusions: Vec<RegexPattern>,
}

impl Repository {
    async fn try_upstreams<'a, F, T>(
        &'a self,
        namespace: &'a Namespace,
        fallback: Error,
        mut op: F,
    ) -> Result<T, Error>
    where
        F: FnMut(
            &'a RegistryClient,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, Error>> + Send + 'a>,
        >,
    {
        for upstream in &self.upstreams {
            match op(upstream).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    warn!(
                        "Upstream operation failed for namespace '{namespace}' against {}: {e}",
                        upstream.url
                    );
                }
            }
        }
        Err(fallback)
    }

    pub async fn new(
        name: &str,
        config: &Config,
        cache: &Arc<Cache>,
        max_manifest_size_bytes: usize,
    ) -> Result<Self, Error> {
        let upstreams = build_upstreams(&config.upstream, cache, max_manifest_size_bytes).await?;
        let replication =
            build_downstreams(name, &config.downstream, cache, max_manifest_size_bytes).await?;
        let retention_policy =
            RetentionPolicy::new(&config.retention_policy, Arc::new(SystemClock));

        Ok(Self {
            name: name.to_string(),
            upstreams,
            replication,
            retention_policy,
            immutable_tags: config.immutable_tags,
            immutable_tags_exclusions: config.immutable_tags_exclusions.clone(),
        })
    }

    pub fn is_pull_through(&self) -> bool {
        !self.upstreams.is_empty()
    }

    /// Checks whether the upstream still has the same digest for the given tag.
    pub async fn is_upstream_digest_match(
        &self,
        accepted_types: &[String],
        namespace: &Namespace,
        reference: &Reference,
        local_digest: &Digest,
    ) -> Result<bool, Error> {
        let (_, upstream_digest, _) = self
            .head_manifest(accepted_types, namespace, reference)
            .await?;
        Ok(upstream_digest == *local_digest)
    }

    #[instrument(skip(self))]
    pub async fn head_blob(
        &self,
        accepted_types: &[String],
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(Digest, u64), Error> {
        self.try_upstreams(namespace, Error::BlobUnknown, |upstream| {
            let location = upstream.get_blob_path(&self.name, namespace.as_ref(), digest);
            Box::pin(async move { upstream.head_blob(accepted_types, &location).await })
        })
        .await
    }

    #[instrument(skip(self))]
    pub async fn get_blob(
        &self,
        accepted_types: &[String],
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(u64, BoxedReader), Error> {
        self.try_upstreams(namespace, Error::BlobUnknown, |upstream| {
            let location = upstream.get_blob_path(&self.name, namespace.as_ref(), digest);
            Box::pin(async move { upstream.get_blob(accepted_types, &location).await })
        })
        .await
    }

    #[instrument(skip(self))]
    pub async fn head_manifest(
        &self,
        accepted_types: &[String],
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<(Option<MediaType>, Digest, u64), Error> {
        self.try_upstreams(namespace, Error::ManifestUnknown, |upstream| {
            let location = upstream.get_manifest_path(&self.name, namespace.as_ref(), reference);
            Box::pin(async move { upstream.head_manifest(accepted_types, &location).await })
        })
        .await
    }

    #[instrument(skip(self))]
    pub async fn get_manifest(
        &self,
        accepted_types: &[String],
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<(Option<MediaType>, Digest, Vec<u8>), Error> {
        self.try_upstreams(namespace, Error::ManifestUnknown, |upstream| {
            let location = upstream.get_manifest_path(&self.name, namespace.as_ref(), reference);
            Box::pin(async move { upstream.get_manifest(accepted_types, &location).await })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use tokio::{io::AsyncReadExt, time::timeout};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{
        cache,
        oci::{Digest, Namespace, Reference, Tag},
        registry::{
            Error,
            manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES,
            repository::{Config, RegistryClientConfig, Repository},
        },
        replication::{ReplicationDownstreamConfig, ReplicationMode},
        test_fixtures::webhook::ca_bundle_pem,
    };

    const TEST_DIGEST: &str =
        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    const FALLBACK_DIGEST: &str =
        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    const MANIFEST_PATH: &str = "/v2/repo/manifests/latest";
    const BLOB_PATH: &str =
        "/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    fn registry_client_config(url: String) -> RegistryClientConfig {
        RegistryClientConfig {
            url,
            max_redirect: 5,
            server_ca_bundle: None,
            client_certificate: None,
            client_private_key: None,
            username: None,
            password: None,
        }
    }

    fn downstream_config(name: &str) -> ReplicationDownstreamConfig {
        ReplicationDownstreamConfig {
            name: name.to_string(),
            client: registry_client_config("https://downstream.example.test".to_string()),
            mode: ReplicationMode::default(),
            namespace_filter: Vec::new(),
            max_concurrent_pushes: None,
            prune: false,
        }
    }

    #[test]
    fn split_registry_url_extracts_namespace_prefix_from_the_path() {
        assert_eq!(
            super::split_registry_url("http://192.168.178.143:8000/push-through-1"),
            (
                "http://192.168.178.143:8000".to_string(),
                "push-through-1".to_string()
            )
        );
        assert_eq!(
            super::split_registry_url("https://host:8000/team/sub/"),
            ("https://host:8000".to_string(), "team/sub".to_string())
        );
    }

    #[test]
    fn split_registry_url_yields_no_prefix_for_a_bare_host() {
        assert_eq!(
            super::split_registry_url("http://angos-b:8000"),
            ("http://angos-b:8000".to_string(), String::new())
        );
        assert_eq!(
            super::split_registry_url("https://angos-eu.example.com/"),
            ("https://angos-eu.example.com".to_string(), String::new())
        );
    }

    async fn repository_with_upstreams(first_url: String, second_url: String) -> Repository {
        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![
                registry_client_config(first_url),
                registry_client_config(second_url),
            ],
            ..Default::default()
        };

        Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap()
    }

    async fn mount_response(
        server: &MockServer,
        request_method: &str,
        request_path: &str,
        response: ResponseTemplate,
    ) {
        Mock::given(method(request_method))
            .and(path(request_path))
            .respond_with(response)
            .mount(server)
            .await;
    }

    async fn fallback_repository(
        request_method: &str,
        request_path: &str,
        response: ResponseTemplate,
    ) -> (Repository, MockServer, MockServer) {
        let first = MockServer::start().await;
        let second = MockServer::start().await;

        mount_response(
            &first,
            request_method,
            request_path,
            ResponseTemplate::new(404),
        )
        .await;
        mount_response(&second, request_method, request_path, response).await;

        let repository = repository_with_upstreams(first.uri(), second.uri()).await;
        (repository, first, second)
    }

    #[tokio::test]
    async fn test_is_pull_through_empty() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config::default();
        let repo = Repository::new("test", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();

        assert!(!repo.is_pull_through());
    }

    #[tokio::test]
    async fn duplicate_downstream_name_is_rejected() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            downstream: vec![downstream_config("eu"), downstream_config("eu")],
            ..Default::default()
        };

        let error = Repository::new("repo", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .err()
            .expect("duplicate downstream name must be rejected");

        match error {
            Error::Initialization(message) => {
                assert!(message.contains("duplicate"), "message: {message}");
                assert!(message.contains("eu"), "message: {message}");
                assert!(message.contains("repo"), "message: {message}");
            }
            other => panic!("expected Error::Initialization, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_downstream_name_is_rejected() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            downstream: vec![downstream_config("")],
            ..Default::default()
        };

        let error = Repository::new("repo", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .err()
            .expect("empty downstream name must be rejected");

        match error {
            Error::Initialization(message) => {
                assert!(message.contains("empty name"), "message: {message}");
                assert!(message.contains("repo"), "message: {message}");
            }
            other => panic!("expected Error::Initialization, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_is_pull_through_with_upstreams() {
        let mock_server = MockServer::start().await;
        let cache = cache::Config::Memory.to_backend().unwrap();

        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("test", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        assert!(repo.is_pull_through());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn new_loads_upstream_tls_files_on_single_worker_runtime() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let ca_bundle_path = tmp_dir.path().join("ca.pem");
        fs::write(&ca_bundle_path, ca_bundle_pem()).unwrap();

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: "https://registry.example.test".to_string(),
                max_redirect: 5,
                server_ca_bundle: Some(ca_bundle_path.to_string_lossy().to_string()),
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repository = timeout(
            Duration::from_secs(2),
            Repository::new("test", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(repository.is_pull_through());
    }

    #[tokio::test]
    async fn test_head_manifest_success_first_upstream() {
        let mock_server = MockServer::start().await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/manifests/latest"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", "1234")
                    .insert_header(
                        "Docker-Content-Digest",
                        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    ),
            )
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.head_manifest(&[], &namespace, &reference).await;
        assert!(result.is_ok());

        let (content_type, digest, size) = result.unwrap();
        assert_eq!(content_type, None);
        assert_eq!(size, 1234);
        assert_eq!(
            digest,
            Digest::try_from(
                "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_fallback_to_second_upstream() {
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let namespace = Namespace::new("local/repo").unwrap();

        let (repo, _first, _second) = fallback_repository(
            "HEAD",
            MANIFEST_PATH,
            ResponseTemplate::new(200)
                .insert_header("Content-Length", "5678")
                .insert_header("Docker-Content-Digest", FALLBACK_DIGEST),
        )
        .await;
        let result = repo.head_manifest(&[], &namespace, &reference).await;
        assert!(result.is_ok());

        let (_content_type, digest, size) = result.unwrap();
        assert_eq!(size, 5678);
        assert_eq!(digest, Digest::try_from(FALLBACK_DIGEST).unwrap());

        let manifest_content = b"{\"schemaVersion\":2}";
        let (repo, _first, _second) = fallback_repository(
            "GET",
            MANIFEST_PATH,
            ResponseTemplate::new(200)
                .set_body_bytes(manifest_content)
                .insert_header("Docker-Content-Digest", TEST_DIGEST),
        )
        .await;
        let result = repo.get_manifest(&[], &namespace, &reference).await;
        assert!(result.is_ok());

        let (_content_type, _digest, body) = result.unwrap();
        assert_eq!(body, manifest_content);

        let digest = Digest::try_from(TEST_DIGEST).unwrap();
        let (repo, _first, _second) = fallback_repository(
            "HEAD",
            BLOB_PATH,
            ResponseTemplate::new(200)
                .insert_header("Content-Length", "5432")
                .insert_header("Docker-Content-Digest", TEST_DIGEST),
        )
        .await;
        let result = repo.head_blob(&[], &namespace, &digest).await;
        assert!(result.is_ok());

        let (_returned_digest, size) = result.unwrap();
        assert_eq!(size, 5432);

        let blob_content = b"blob data here";
        let (repo, _first, _second) = fallback_repository(
            "GET",
            BLOB_PATH,
            ResponseTemplate::new(200).set_body_bytes(blob_content),
        )
        .await;
        let result = repo.get_blob(&[], &namespace, &digest).await;
        assert!(result.is_ok());

        let (size, mut reader) = result.unwrap();
        assert_eq!(size, blob_content.len() as u64);

        let mut buffer = Vec::new();
        AsyncReadExt::read_to_end(&mut reader, &mut buffer)
            .await
            .unwrap();
        assert_eq!(buffer, blob_content);
    }

    #[tokio::test]
    async fn test_head_manifest_all_upstreams_fail() {
        let mock_server = MockServer::start().await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/manifests/latest"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.head_manifest(&[], &namespace, &reference).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ManifestUnknown));
    }

    #[tokio::test]
    async fn test_get_manifest_success() {
        let mock_server = MockServer::start().await;
        let manifest_content = b"{\"schemaVersion\":2}";

        Mock::given(method("GET"))
            .and(path("/v2/repo/manifests/latest"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(manifest_content)
                    .insert_header(
                        "Docker-Content-Digest",
                        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    ),
            )
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.get_manifest(&[], &namespace, &reference).await;
        assert!(result.is_ok());

        let (_content_type, _digest, body) = result.unwrap();
        assert_eq!(body, manifest_content);
    }

    #[tokio::test]
    async fn test_get_manifest_all_upstreams_fail() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v2/repo/manifests/latest"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.get_manifest(&[], &namespace, &reference).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ManifestUnknown));
    }

    #[tokio::test]
    async fn test_head_blob_success_first_upstream() {
        let mock_server = MockServer::start().await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", "9876")
                    .insert_header(
                        "Docker-Content-Digest",
                        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    ),
            )
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.head_blob(&[], &namespace, &digest).await;
        assert!(result.is_ok());

        let (returned_digest, size) = result.unwrap();
        assert_eq!(size, 9876);
        assert_eq!(returned_digest, digest);
    }

    #[tokio::test]
    async fn test_head_blob_all_upstreams_fail() {
        let mock_server = MockServer::start().await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.head_blob(&[], &namespace, &digest).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::BlobUnknown));
    }

    #[tokio::test]
    async fn test_get_blob_success() {
        let mock_server = MockServer::start().await;
        let blob_content = b"blob data here";

        Mock::given(method("GET"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(blob_content))
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.get_blob(&[], &namespace, &digest).await;
        assert!(result.is_ok());

        let (size, mut reader) = result.unwrap();
        assert_eq!(size, blob_content.len() as u64);

        let mut buffer = Vec::new();
        AsyncReadExt::read_to_end(&mut reader, &mut buffer)
            .await
            .unwrap();
        assert_eq!(buffer, blob_content);
    }

    #[tokio::test]
    async fn test_get_blob_all_upstreams_fail() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                url: mock_server.uri(),
                max_redirect: 5,
                server_ca_bundle: None,
                client_certificate: None,
                client_private_key: None,
                username: None,
                password: None,
            }],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();
        let namespace = Namespace::new("local/repo").unwrap();

        let result = repo.get_blob(&[], &namespace, &digest).await;
        assert!(result.is_err());
        match result {
            Err(Error::BlobUnknown) => (),
            _ => panic!("Expected Error::BlobUnknown"),
        }
    }
}
