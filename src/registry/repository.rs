use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use tokio::task;
use tracing::{instrument, warn};

use angos_cache::Cache;
use angos_oci::http_range::RequestRange;
use angos_oci::request::{
    GetBlobRequest, GetManifestRequest, GetReferrersRequest, HeadBlobRequest, HeadManifestRequest,
};
use angos_oci::response::{ManifestHeadResponse, ManifestResponse};
use angos_oci::{Descriptor, Digest, Error as OciError, MediaRange, Namespace, Reference};
use angos_oci_client::{Error as ClientError, FetchedBlob, RegistryClient, RegistryClientConfig};

use crate::configuration::RegexPattern;
use crate::layer::IndexAction;
use crate::policy::{
    AccessMode, ImagePolicy, PolicyConfig, RetentionPolicy, RetentionPolicyConfig,
};
use crate::registry::Error;
use crate::replication::{ReplicationDownstream, ReplicationDownstreamConfig};
use crate::scan::ScanAction;

/// Fallback per-manifest blob-push and top-level child-manifest concurrency
/// when a downstream omits `max_concurrent_pushes`.
const DEFAULT_MAX_CONCURRENT_PUSHES: NonZeroUsize = NonZeroUsize::MIN.saturating_add(4 - 1);

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

/// Resolve one upstream/downstream client config: split the URL into the
/// registry base and its namespace prefix, validate the prefix, and build the
/// client against the base URL.
fn build_client(
    config: &RegistryClientConfig,
    repo_name: &Namespace,
    cache: &Arc<Cache>,
    max_manifest_size_bytes: usize,
) -> Result<(RegistryClient, Option<Namespace>), Error> {
    let (base_url, prefix) = split_registry_url(&config.url);
    // A misconfigured url path fails at startup, not at request time.
    let target_namespace = if prefix.is_empty() {
        None
    } else {
        Some(Namespace::new(&prefix).map_err(|e| {
            Error::Initialization(format!(
                "repository '{repo_name}' has a url path '{prefix}' that is not a valid namespace prefix: {e}"
            ))
        })?)
    };
    let mut client_config = config.clone();
    client_config.url = base_url;
    let client =
        RegistryClient::from_config(&client_config, Arc::clone(cache), max_manifest_size_bytes)?;
    Ok((client, target_namespace))
}

/// The upstream and downstream clients. A path on an upstream URL is prepended
/// to the namespace once the repository `name` is stripped; on a downstream URL
/// it replaces this repository's prefix (`repo/x` -> `prefix/x`), and a
/// bare-host downstream mirrors the namespace verbatim.
fn build_clients(
    name: &Namespace,
    upstream_configs: &[RegistryClientConfig],
    downstream_configs: &[ReplicationDownstreamConfig],
    cache: &Arc<Cache>,
    max_manifest_size_bytes: usize,
) -> Result<(Vec<Upstream>, Vec<ReplicationDownstream>), Error> {
    let mut upstreams = Vec::new();
    for config in upstream_configs {
        let (client, target_namespace) =
            build_client(config, name, cache, max_manifest_size_bytes)?;
        upstreams.push(Upstream {
            client,
            local_namespace: name.clone(),
            target_namespace,
        });
    }
    let mut downstreams = Vec::new();
    for config in downstream_configs {
        let (registry_client, target_namespace) =
            build_client(&config.client, name, cache, max_manifest_size_bytes)?;
        downstreams.push(ReplicationDownstream {
            name: config.name.clone(),
            registry_client: Arc::new(registry_client),
            mode: config.mode,
            namespace_filter: config
                .namespace_filter
                .iter()
                .cloned()
                .map(RegexPattern::into_regex)
                .collect(),
            max_concurrent_pushes: config
                .max_concurrent_pushes
                .unwrap_or(DEFAULT_MAX_CONCURRENT_PUSHES),
            prune: config.prune,
            local_namespace: target_namespace.as_ref().map(|_| name.clone()),
            target_namespace,
        });
    }
    Ok((upstreams, downstreams))
}

/// One resolved pull-through upstream: the URL-formatting client plus the
/// namespace mapping (`local_namespace` to strip, optional `target_namespace` to
/// prepend) applied via [`Namespace::remote`].
pub struct Upstream {
    pub client: RegistryClient,
    pub local_namespace: Namespace,
    pub target_namespace: Option<Namespace>,
}

impl Upstream {
    /// Maps `namespace` to its upstream form via [`Namespace::remote`]. For a
    /// routed namespace the strip always succeeds, so an `Err` is effectively
    /// unreachable but callers still propagate it.
    pub fn remote(&self, namespace: &Namespace) -> Result<Namespace, OciError> {
        namespace.remote(Some(&self.local_namespace), self.target_namespace.as_ref())
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    /// The registry namespace this repository mirrors, as a client spells it in
    /// the `?ns=` proxy parameter (`docker.io`). A pull naming it is served from
    /// this repository whatever path it asks for; without it `?ns=` selects
    /// nothing and the local prefix alone decides.
    pub namespace: Option<String>,
    #[serde(default)]
    pub upstream: Vec<RegistryClientConfig>,
    #[serde(default)]
    pub downstream: Vec<ReplicationDownstreamConfig>,
    pub access_policy: Option<PolicyConfig<AccessMode>>,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
    /// Which of the images pushed here are sent to the scanner service, and
    /// when their reports are refreshed; in place of `[global.scan]`'s policy.
    pub scan: Option<PolicyConfig<ScanAction>>,
    /// Which of the images pushed here have their filesystem indexed as they
    /// land rather than when first browsed; in place of `[global.index]`.
    pub index: Option<PolicyConfig<IndexAction>>,
}

impl Config {
    /// The repository's authorization-webhook reference, treating a blank string
    /// as unset. Configuration validation and authorizer construction share this
    /// so `authorization_webhook = ""` resolves to no webhook in both, instead of
    /// passing validation and then missing the authorizer lookup.
    pub fn authorization_webhook_ref(&self) -> Option<&str> {
        self.authorization_webhook
            .as_deref()
            .filter(|name| !name.is_empty())
    }
}

pub struct Repository {
    pub name: Namespace,
    /// The registry namespace this repository mirrors, which is the `?ns=`
    /// value it answers to. Distinct from `name`, the local prefix it owns.
    pub namespace: Option<String>,
    pub upstreams: Vec<Upstream>,
    pub replication: Vec<ReplicationDownstream>,
    pub retention_policy: RetentionPolicy,
    pub immutable_tags: bool,
    pub immutable_tags_exclusions: Vec<RegexPattern>,
    /// Which images are scanned, and when their reports are refreshed;
    /// `None` scans nothing.
    pub scan: Option<ImagePolicy>,
    /// Which images have their filesystem indexed as they land; `None`
    /// indexes on demand only.
    pub index: Option<ImagePolicy>,
}

impl Repository {
    /// Runs `op` against each upstream in turn, the namespace mapped to that
    /// upstream's form, and answers with the first success. With none, the
    /// answer is `fallback` when every upstream answered, and
    /// [`Error::UpstreamUnavailable`] when one could not: the content may be
    /// there, so a miss would be a guess.
    async fn try_upstreams<'a, F, T>(
        &'a self,
        namespace: &'a Namespace,
        fallback: Error,
        op: F,
    ) -> Result<T, Error>
    where
        F: Fn(&'a RegistryClient, Namespace) -> BoxFuture<'a, Result<T, ClientError>>,
    {
        let mut unavailable = false;
        for upstream in &self.upstreams {
            let error = match upstream.remote(namespace) {
                Ok(remote) => match op(&upstream.client, remote).await {
                    Ok(result) => return Ok(result),
                    // A miss or a refusal is an answer; `Internal` is the
                    // client's word for none: a transport failure, an
                    // unexpected status such as a 5xx or 429, or a response
                    // it could not read.
                    Err(error) => {
                        unavailable |= matches!(error, ClientError::Internal(_));
                        Error::from(error)
                    }
                },
                Err(error) => error.into(),
            };
            warn!(
                "Upstream operation failed for namespace '{namespace}' against {}: {error}",
                upstream.client.url
            );
        }
        if unavailable {
            return Err(Error::UpstreamUnavailable(format!(
                "no upstream of '{}' answered for '{namespace}'",
                self.name
            )));
        }
        Err(fallback)
    }

    pub async fn new(
        name: &str,
        config: &Config,
        cache: &Arc<Cache>,
        max_manifest_size_bytes: usize,
    ) -> Result<Self, Error> {
        let name = Namespace::new(name).map_err(|e| {
            Error::Initialization(format!(
                "repository name '{name}' is not a valid namespace: {e}"
            ))
        })?;
        let mut seen_names = HashSet::new();
        for downstream in &config.downstream {
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

        let (upstreams, replication) = if config.upstream.is_empty() && config.downstream.is_empty()
        {
            (Vec::new(), Vec::new())
        } else {
            // Building a client reads its TLS files, which blocks.
            let (upstream_configs, downstream_configs) =
                (config.upstream.clone(), config.downstream.clone());
            let (cache, repo_name) = (Arc::clone(cache), name.clone());
            task::spawn_blocking(move || {
                build_clients(
                    &repo_name,
                    &upstream_configs,
                    &downstream_configs,
                    &cache,
                    max_manifest_size_bytes,
                )
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to initialize registry clients: {e}")))??
        };
        let retention_policy = RetentionPolicy::new(config.retention_policy.rules.clone());

        Ok(Self {
            name,
            // Lowercased once here: `ns` names a host, and a host is
            // case-insensitive, so the resolver never has to care.
            namespace: config
                .namespace
                .as_deref()
                .filter(|namespace| !namespace.is_empty())
                .map(str::to_ascii_lowercase),
            upstreams,
            replication,
            retention_policy,
            immutable_tags: config.immutable_tags,
            immutable_tags_exclusions: config.immutable_tags_exclusions.clone(),
            scan: config.scan.as_ref().map(ImagePolicy::new),
            index: config.index.as_ref().map(ImagePolicy::new),
        })
    }

    pub fn is_pull_through(&self) -> bool {
        !self.upstreams.is_empty()
    }

    #[instrument(skip(self))]
    pub async fn head_blob(
        &self,
        accepted_types: &[MediaRange],
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<(Digest, u64), Error> {
        self.try_upstreams(namespace, Error::BlobUnknown, |client, remote| {
            Box::pin(client.head_blob(HeadBlobRequest {
                namespace: remote,
                digest: digest.clone(),
                accepted_types: accepted_types.to_vec(),
            }))
        })
        .await
    }

    /// Lists a subject's referrers on the first upstream that answers, so a
    /// pull-through namespace can advertise what the upstream holds alongside
    /// what it cached.
    #[instrument(skip(self))]
    pub async fn list_referrers(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<Vec<Descriptor>, Error> {
        self.try_upstreams(namespace, Error::ManifestUnknown, |client, remote| {
            Box::pin(client.list_referrers(GetReferrersRequest {
                namespace: remote,
                digest: digest.clone(),
                artifact_type: None,
                last: None,
            }))
        })
        .await
    }

    /// Streams a blob from the first upstream that serves it, forwarding the
    /// client's `Range` when there is one.
    #[instrument(skip(self))]
    pub async fn get_blob(
        &self,
        accepted_types: &[MediaRange],
        namespace: &Namespace,
        digest: &Digest,
        range: Option<RequestRange>,
    ) -> Result<FetchedBlob, Error> {
        self.try_upstreams(namespace, Error::BlobUnknown, |client, remote| {
            Box::pin(client.get_blob(GetBlobRequest {
                namespace: remote,
                digest: digest.clone(),
                accepted_types: accepted_types.to_vec(),
                range,
            }))
        })
        .await
    }

    #[instrument(skip(self))]
    pub async fn head_manifest(
        &self,
        accepted_types: &[MediaRange],
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<ManifestHeadResponse, Error> {
        self.try_upstreams(namespace, Error::ManifestUnknown, |client, remote| {
            Box::pin(client.head_manifest(HeadManifestRequest {
                namespace: remote,
                reference: reference.clone(),
                accepted_types: accepted_types.to_vec(),
            }))
        })
        .await
    }

    #[instrument(skip(self))]
    pub async fn get_manifest(
        &self,
        accepted_types: &[MediaRange],
        namespace: &Namespace,
        reference: &Reference,
    ) -> Result<ManifestResponse, Error> {
        self.try_upstreams(namespace, Error::ManifestUnknown, |client, remote| {
            Box::pin(client.get_manifest(GetManifestRequest {
                namespace: remote,
                reference: reference.clone(),
                accepted_types: accepted_types.to_vec(),
            }))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use tokio::io::AsyncReadExt;
    use tokio::time::timeout;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use angos_cache::Config as CacheConfig;
    use angos_mtls_client::ClientTls;
    use angos_oci::{Digest, Namespace, Reference, Tag};
    use angos_oci_client::RegistryClientConfig;

    use crate::registry::Error;
    use crate::registry::manifest::DEFAULT_MAX_MANIFEST_SIZE_BYTES;
    use crate::registry::repository::{Config, Repository, split_registry_url};
    use crate::replication::{ReplicationDownstreamConfig, ReplicationMode};
    use crate::test_fixtures::client::test_client_config;
    use crate::test_fixtures::webhook::ca_bundle_pem;

    const TEST_DIGEST: &str =
        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    const FALLBACK_DIGEST: &str =
        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    const MANIFEST_PATH: &str = "/v2/repo/manifests/latest";
    const BLOB_PATH: &str =
        "/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    fn downstream_config(name: &str) -> ReplicationDownstreamConfig {
        ReplicationDownstreamConfig {
            name: name.to_string(),
            client: test_client_config("https://downstream.example.test".to_string()),
            mode: ReplicationMode::default(),
            namespace_filter: Vec::new(),
            max_concurrent_pushes: None,
            prune: false,
        }
    }

    #[test]
    fn split_registry_url_moves_the_path_into_the_namespace_prefix() {
        assert_eq!(
            split_registry_url("https://host:8000/team/sub/"),
            ("https://host:8000".to_string(), "team/sub".to_string())
        );
        assert_eq!(
            split_registry_url("http://angos-b:8000"),
            ("http://angos-b:8000".to_string(), String::new())
        );
        assert_eq!(
            split_registry_url("https://angos-eu.example.com/"),
            ("https://angos-eu.example.com".to_string(), String::new())
        );
    }

    #[tokio::test]
    async fn an_upstream_url_path_that_is_no_namespace_fails_at_startup() {
        let cache = CacheConfig::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![test_client_config(
                "http://upstream.example.test/Bad_Prefix".to_string(),
            )],
            ..Default::default()
        };

        assert!(matches!(
            Repository::new("repo", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES).await,
            Err(Error::Initialization(_))
        ));
    }

    /// A repository whose first upstream answers 404 and second `response`.
    async fn fallback_repository(
        request_method: &str,
        request_path: &str,
        response: ResponseTemplate,
    ) -> (Repository, MockServer, MockServer) {
        let first = MockServer::start().await;
        let second = MockServer::start().await;
        for (server, response) in [(&first, ResponseTemplate::new(404)), (&second, response)] {
            Mock::given(method(request_method))
                .and(path(request_path))
                .respond_with(response)
                .mount(server)
                .await;
        }

        let cache = CacheConfig::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![
                test_client_config(first.uri()),
                test_client_config(second.uri()),
            ],
            ..Default::default()
        };
        let repository = Repository::new("local", &config, &cache, DEFAULT_MAX_MANIFEST_SIZE_BYTES)
            .await
            .unwrap();
        (repository, first, second)
    }

    #[tokio::test]
    async fn duplicate_downstream_name_is_rejected() {
        let cache = CacheConfig::Memory.to_backend().unwrap();
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
        let cache = CacheConfig::Memory.to_backend().unwrap();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn new_loads_upstream_tls_files_on_single_worker_runtime() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let ca_bundle_path = tmp_dir.path().join("ca.pem");
        fs::write(&ca_bundle_path, ca_bundle_pem()).unwrap();

        let cache = CacheConfig::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![RegistryClientConfig {
                tls: ClientTls {
                    server_ca_bundle: Some(ca_bundle_path.clone()),
                    identity: None,
                },
                ..test_client_config("https://registry.example.test".to_string())
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

    /// An upstream that omits `Docker-Content-Digest` answers the freshness
    /// probe with no digest, which reads as a mismatch, instead of failing it.
    #[tokio::test]
    async fn upstream_omitting_the_digest_header_answers_without_a_digest() {
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let namespace = Namespace::new("local/repo").unwrap();

        let (repo, _first, _second) = fallback_repository(
            "HEAD",
            MANIFEST_PATH,
            ResponseTemplate::new(200).insert_header("Content-Length", "5678"),
        )
        .await;

        let head = repo
            .head_manifest(&[], &namespace, &reference)
            .await
            .expect("a missing Docker-Content-Digest must not fail the freshness probe");

        assert_eq!(head.digest, None);
    }

    /// A miss is believed only when every upstream answered it: one that could
    /// not answer may hold the content, so the outcome is an outage, blob and
    /// manifest alike.
    #[tokio::test]
    async fn an_upstream_that_cannot_answer_is_unavailable_not_a_miss() {
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let namespace = Namespace::new("local/repo").unwrap();

        let (repo, _first, _second) =
            fallback_repository("HEAD", MANIFEST_PATH, ResponseTemplate::new(503)).await;
        let error = repo
            .head_manifest(&[], &namespace, &reference)
            .await
            .expect_err("no upstream served the manifest");
        assert!(
            matches!(error, Error::UpstreamUnavailable(_)),
            "a 404 beside a 503 is no miss, got: {error:?}"
        );

        let digest = Digest::try_from(TEST_DIGEST).unwrap();
        let (repo, _first, _second) =
            fallback_repository("GET", BLOB_PATH, ResponseTemplate::new(429)).await;
        let error = repo
            .get_blob(&[], &namespace, &digest, None)
            .await
            .err()
            .expect("no upstream served the blob");
        assert!(
            matches!(error, Error::UpstreamUnavailable(_)),
            "a throttled upstream is no miss, got: {error:?}"
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
        let head = result.unwrap();
        assert_eq!(head.size, 5678);
        assert_eq!(
            head.digest,
            Some(Digest::try_from(FALLBACK_DIGEST).unwrap())
        );

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
        let body = result.unwrap().body;
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
        let (_returned_digest, size) = result.unwrap();
        assert_eq!(size, 5432);

        let blob_content = b"blob data here";
        let (repo, _first, _second) = fallback_repository(
            "GET",
            BLOB_PATH,
            ResponseTemplate::new(200).set_body_bytes(blob_content),
        )
        .await;
        let result = repo.get_blob(&[], &namespace, &digest, None).await;
        let fetched = result.unwrap();
        assert_eq!(fetched.length, blob_content.len() as u64);
        let mut reader = fetched.reader;

        let mut buffer = Vec::new();
        AsyncReadExt::read_to_end(&mut reader, &mut buffer)
            .await
            .unwrap();
        assert_eq!(buffer, blob_content);
    }

    /// Every upstream answering a miss is a miss, under the operation's own code.
    #[tokio::test]
    async fn every_upstream_missing_is_unknown() {
        let namespace = Namespace::new("local/repo").unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());

        let (repo, _first, _second) =
            fallback_repository("HEAD", MANIFEST_PATH, ResponseTemplate::new(404)).await;
        let result = repo.head_manifest(&[], &namespace, &reference).await;
        assert!(matches!(result, Err(Error::ManifestUnknown)), "{result:?}");

        let digest = Digest::try_from(TEST_DIGEST).unwrap();
        let (repo, _first, _second) =
            fallback_repository("GET", BLOB_PATH, ResponseTemplate::new(404)).await;
        let result = repo.get_blob(&[], &namespace, &digest, None).await;
        assert!(
            matches!(result, Err(Error::BlobUnknown)),
            "{:?}",
            result.err()
        );
    }
}
