use std::sync::Arc;

use serde::Deserialize;
use tracing::instrument;

use crate::{
    cache::Cache,
    oci::{Digest, Reference},
    policy::{AccessPolicyConfig, RetentionPolicy, RetentionPolicyConfig},
    registry::{Error, blob_store::BoxedReader},
};

mod registry_client;

use registry_client::RegistryClient;
pub use registry_client::RegistryClientConfig;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub upstream: Vec<RegistryClientConfig>,
    #[serde(default)]
    pub access_policy: AccessPolicyConfig,
    #[serde(default)]
    pub retention_policy: RetentionPolicyConfig,
    #[serde(default)]
    pub immutable_tags: bool,
    #[serde(default)]
    pub immutable_tags_exclusions: Vec<String>,
    pub authorization_webhook: Option<String>,
    #[serde(default)]
    pub event_webhooks: Vec<String>,
}

pub struct Repository {
    pub name: String,
    pub upstreams: Vec<RegistryClient>,
    pub retention_policy: RetentionPolicy,
    pub immutable_tags: bool,
    pub immutable_tags_exclusions: Vec<String>,
}

impl Repository {
    pub fn new(name: &str, config: &Config, cache: &Arc<dyn Cache>) -> Result<Self, Error> {
        let mut upstreams = Vec::new();
        for config in &config.upstream {
            upstreams.push(RegistryClient::new(config, cache.clone())?);
        }

        let retention_policy = RetentionPolicy::new(&config.retention_policy)?;

        Ok(Self {
            name: name.to_string(),
            upstreams,
            retention_policy,
            immutable_tags: config.immutable_tags,
            immutable_tags_exclusions: config.immutable_tags_exclusions.clone(),
        })
    }

    pub fn is_pull_through(&self) -> bool {
        !self.upstreams.is_empty()
    }

    #[instrument(skip(self))]
    pub async fn head_blob(
        &self,
        accepted_types: &[String],
        namespace: &str,
        digest: &Digest,
    ) -> Result<(Digest, u64), Error> {
        for upstream in &self.upstreams {
            let location = upstream.get_blob_path(&self.name, namespace, digest);
            let response = upstream.head_blob(accepted_types, &location).await;

            if response.is_ok() {
                return response;
            }
        }

        Err(Error::ManifestUnknown)
    }

    #[instrument(skip(self))]
    pub async fn get_blob(
        &self,
        accepted_types: &[String],
        namespace: &str,
        digest: &Digest,
    ) -> Result<(u64, BoxedReader), Error> {
        for upstream in &self.upstreams {
            let location = upstream.get_blob_path(&self.name, namespace, digest);
            if let Ok(response) = upstream.get_blob(accepted_types, &location).await {
                return Ok(response);
            }
        }

        Err(Error::ManifestUnknown)
    }

    #[instrument(skip(self))]
    pub async fn head_manifest(
        &self,
        accepted_types: &[String],
        namespace: &str,
        reference: &Reference,
    ) -> Result<(Option<String>, Digest, u64), Error> {
        for upstream in &self.upstreams {
            let location = upstream.get_manifest_path(&self.name, namespace, reference);
            if let Ok(response) = upstream.head_manifest(accepted_types, &location).await {
                return Ok(response);
            }
        }

        Err(Error::ManifestUnknown)
    }

    #[instrument(skip(self))]
    pub async fn get_manifest(
        &self,
        accepted_types: &[String],
        namespace: &str,
        reference: &Reference,
    ) -> Result<(Option<String>, Digest, Vec<u8>), Error> {
        for upstream in &self.upstreams {
            let location = upstream.get_manifest_path(&self.name, namespace, reference);
            if let Ok(response) = upstream.get_manifest(accepted_types, &location).await {
                return Ok(response);
            }
        }

        Err(Error::ManifestUnknown)
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::cache;

    #[tokio::test]
    async fn test_is_pull_through_empty() {
        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config::default();
        let repo = Repository::new("test", &config, &cache).unwrap();

        assert!(!repo.is_pull_through());
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

        let repo = Repository::new("test", &config, &cache).unwrap();
        assert!(repo.is_pull_through());
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let reference = Reference::Tag("latest".to_string());

        let result = repo.head_manifest(&[], "local/repo", &reference).await;
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
    async fn test_head_manifest_fallback_to_second_upstream() {
        let mock_server1 = MockServer::start().await;
        let mock_server2 = MockServer::start().await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/manifests/latest"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server1)
            .await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/manifests/latest"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", "5678")
                    .insert_header(
                        "Docker-Content-Digest",
                        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    ),
            )
            .mount(&mock_server2)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![
                RegistryClientConfig {
                    url: mock_server1.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
                RegistryClientConfig {
                    url: mock_server2.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
            ],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache).unwrap();
        let reference = Reference::Tag("latest".to_string());

        let result = repo.head_manifest(&[], "local/repo", &reference).await;
        assert!(result.is_ok());

        let (_content_type, digest, size) = result.unwrap();
        assert_eq!(size, 5678);
        assert_eq!(
            digest,
            Digest::try_from(
                "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
            )
            .unwrap()
        );
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let reference = Reference::Tag("latest".to_string());

        let result = repo.head_manifest(&[], "local/repo", &reference).await;
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let reference = Reference::Tag("latest".to_string());

        let result = repo.get_manifest(&[], "local/repo", &reference).await;
        assert!(result.is_ok());

        let (_content_type, _digest, body) = result.unwrap();
        assert_eq!(body, manifest_content);
    }

    #[tokio::test]
    async fn test_get_manifest_fallback_to_second_upstream() {
        let mock_server1 = MockServer::start().await;
        let mock_server2 = MockServer::start().await;
        let manifest_content = b"{\"schemaVersion\":2}";

        Mock::given(method("GET"))
            .and(path("/v2/repo/manifests/latest"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server1)
            .await;

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
            .mount(&mock_server2)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![
                RegistryClientConfig {
                    url: mock_server1.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
                RegistryClientConfig {
                    url: mock_server2.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
            ],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache).unwrap();
        let reference = Reference::Tag("latest".to_string());

        let result = repo.get_manifest(&[], "local/repo", &reference).await;
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let reference = Reference::Tag("latest".to_string());

        let result = repo.get_manifest(&[], "local/repo", &reference).await;
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        let result = repo.head_blob(&[], "local/repo", &digest).await;
        assert!(result.is_ok());

        let (returned_digest, size) = result.unwrap();
        assert_eq!(size, 9876);
        assert_eq!(returned_digest, digest);
    }

    #[tokio::test]
    async fn test_head_blob_fallback_to_second_upstream() {
        let mock_server1 = MockServer::start().await;
        let mock_server2 = MockServer::start().await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server1)
            .await;

        Mock::given(method("HEAD"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", "5432")
                    .insert_header(
                        "Docker-Content-Digest",
                        "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                    ),
            )
            .mount(&mock_server2)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![
                RegistryClientConfig {
                    url: mock_server1.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
                RegistryClientConfig {
                    url: mock_server2.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
            ],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache).unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        let result = repo.head_blob(&[], "local/repo", &digest).await;
        assert!(result.is_ok());

        let (_returned_digest, size) = result.unwrap();
        assert_eq!(size, 5432);
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        let result = repo.head_blob(&[], "local/repo", &digest).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ManifestUnknown));
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        let result = repo.get_blob(&[], "local/repo", &digest).await;
        assert!(result.is_ok());

        let (size, mut reader) = result.unwrap();
        assert_eq!(size, blob_content.len() as u64);

        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer)
            .await
            .unwrap();
        assert_eq!(buffer, blob_content);
    }

    #[tokio::test]
    async fn test_get_blob_fallback_to_second_upstream() {
        let mock_server1 = MockServer::start().await;
        let mock_server2 = MockServer::start().await;
        let blob_content = b"blob data here";

        Mock::given(method("GET"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server1)
            .await;

        Mock::given(method("GET"))
            .and(path("/v2/repo/blobs/sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(blob_content))
            .mount(&mock_server2)
            .await;

        let cache = cache::Config::Memory.to_backend().unwrap();
        let config = Config {
            upstream: vec![
                RegistryClientConfig {
                    url: mock_server1.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
                RegistryClientConfig {
                    url: mock_server2.uri(),
                    max_redirect: 5,
                    server_ca_bundle: None,
                    client_certificate: None,
                    client_private_key: None,
                    username: None,
                    password: None,
                },
            ],
            ..Default::default()
        };

        let repo = Repository::new("local", &config, &cache).unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        let result = repo.get_blob(&[], "local/repo", &digest).await;
        assert!(result.is_ok());

        let (size, mut reader) = result.unwrap();
        assert_eq!(size, blob_content.len() as u64);

        let mut buffer = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buffer)
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

        let repo = Repository::new("local", &config, &cache).unwrap();
        let digest = Digest::try_from(
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )
        .unwrap();

        let result = repo.get_blob(&[], "local/repo", &digest).await;
        assert!(result.is_err());
        match result {
            Err(Error::ManifestUnknown) => (),
            _ => panic!("Expected Error::ManifestUnknown"),
        }
    }
}
