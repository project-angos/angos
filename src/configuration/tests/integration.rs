use std::io::Write;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use angos_cache::Config as CacheConfig;
use tempfile::NamedTempFile;

use crate::configuration::listeners::ClientAuth;
use crate::configuration::{Configuration, Error, ServerConfig};
use crate::policy::AccessMode;
use crate::registry::blob_store::{BlobStoreConfig, FsBackendConfig};
use crate::replication::ReplicationMode;

#[test]
fn test_load_minimal_config() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [blob_store.fs]
    root_dir = "/var/lib/registry"
    "#;

    let config = Configuration::load_from_str(config).unwrap();

    assert_eq!(
        config.global.max_concurrent_requests,
        NonZeroUsize::new(64).unwrap()
    );
    assert!(!config.global.update_pull_time);

    let ServerConfig::Insecure(server_config) = config.server else {
        panic!("Expected Insecure server config");
    };

    let bind_address = server_config.bind_address.to_string();
    assert_eq!(bind_address, "0.0.0.0".to_string());
    assert_eq!(server_config.port, 8000);
    assert_eq!(server_config.query_timeout.get(), 3600);
    assert_eq!(server_config.query_timeout_grace_period.get(), 60);

    assert_eq!(config.cache, CacheConfig::Memory);
    assert_eq!(
        config.blob_store,
        BlobStoreConfig::FS(FsBackendConfig {
            root_dir: PathBuf::from("/var/lib/registry"),
            sync_to_disk: false,
        })
    );

    assert!(config.auth.identity.is_empty());
    assert!(config.repository.is_empty());
    assert!(config.observability.is_none());
}

/// A `[blob_store]` is mandatory: defaulting to the working directory would
/// write blobs to a container's ephemeral layer and lose them on restart.
#[test]
fn a_configuration_without_a_blob_store_is_refused() {
    let error = Configuration::load_from_str(
        r#"
    [server]
    bind_address = "0.0.0.0"
    "#,
    )
    .expect_err("a configuration naming no storage must not load");

    assert!(
        format!("{error}").contains("blob_store"),
        "the error must name the missing section, got: {error}"
    );
}

#[test]
fn an_empty_blob_store_root_is_refused() {
    let error = Configuration::load_from_str(
        r#"
    [server]
    bind_address = "0.0.0.0"

    [blob_store.fs]
    root_dir = ""
    "#,
    )
    .expect_err("an empty root_dir must not load");

    assert!(
        format!("{error}").contains("root_dir"),
        "the error must name the offending key, got: {error}"
    );
}

#[test]
fn test_repository_config() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [auth.webhook.repo-auth]
    url = "https://auth.example.com/authorize"
    timeout_ms = 1000

    [event_webhook.repo-events]
    url = "https://events.example.com/hook"
    policy = "optional"
    events = ["manifest.push"]

    [repository.myapp]
    immutable_tags = true
    immutable_tags_exclusions = ["dev"]
    authorization_webhook = "repo-auth"
    event_webhooks = ["repo-events"]

    [[repository.myapp.upstream]]
    url = "https://registry.example.com"
    max_redirect = 3
    username = "mirror"
    password = "secret"

    [repository.myapp.access_policy]
    default = "allow"
    rules = ["identity.username == 'admin'"]

    [repository.myapp.retention_policy]
    rules = ["image.tag == 'latest'"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert_eq!(config.repository.len(), 1);
    let repo = &config.repository["myapp"];
    assert_eq!(repo.upstream.len(), 1);
    assert_eq!(repo.upstream[0].url, "https://registry.example.com");
    assert_eq!(repo.upstream[0].max_redirect, 3);
    let auth = repo.upstream[0].basic_auth.as_ref().unwrap();
    assert_eq!(auth.username, "mirror");
    assert_eq!(auth.password.expose(), "secret");
    let access_policy = repo.access_policy.as_ref().unwrap();
    assert_eq!(access_policy.default, Some(AccessMode::Allow));
    assert_eq!(access_policy.rules.len(), 1);
    assert_eq!(repo.retention_policy.rules.len(), 1);
    assert!(repo.immutable_tags);
    assert_eq!(repo.immutable_tags_exclusions.len(), 1);
    assert_eq!(repo.immutable_tags_exclusions[0].as_source(), "dev");
    assert_eq!(repo.authorization_webhook.as_deref(), Some("repo-auth"));
    assert_eq!(repo.event_webhooks, ["repo-events"]);
}

#[test]
fn test_repository_downstream_config() {
    // Downstream tables must parse through the full Configuration, including
    // the repository map visitor and the flattened RegistryClientConfig.
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [[repository.myapp.downstream]]
    name = "instance-b"
    url = "https://angos-eu.example.com"
    username = "replicator"
    password = "s3cret"
    mode = "event-only"
    namespace_filter = ["^nginx/.*"]
    max_concurrent_pushes = 8
    prune = true
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    let repo = &config.repository["myapp"];
    assert_eq!(repo.downstream.len(), 1);
    let downstream = &repo.downstream[0];
    assert_eq!(downstream.name, "instance-b");
    assert_eq!(downstream.client.url, "https://angos-eu.example.com");
    let auth = downstream.client.basic_auth.as_ref().unwrap();
    assert_eq!(auth.username, "replicator");
    assert_eq!(auth.password.expose(), "s3cret");
    assert_eq!(downstream.mode, ReplicationMode::EventOnly);
    assert_eq!(downstream.namespace_filter.len(), 1);
    assert_eq!(downstream.namespace_filter[0].as_source(), "^nginx/.*");
    assert_eq!(downstream.max_concurrent_pushes, NonZeroUsize::new(8));
    assert!(downstream.prune);
}

#[test]
fn test_repository_downstream_rejects_partial_mtls() {
    // The mTLS-pairing validation must fire through the full parse too.
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [[repository.myapp.downstream]]
    name = "instance-b"
    url = "https://angos-eu.example.com"
    client_certificate = "cert.pem"
    "#;

    let result = Configuration::load_from_str(config);
    assert!(
        result.is_err(),
        "partial mTLS in a downstream must be rejected at full-config parse"
    );
}

#[test]
fn test_invalid_toml_format() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server
    bind_address = "0.0.0.0"
    "#;

    let result = Configuration::load_from_str(config);
    assert!(result.is_err());
    match result {
        Err(Error::InvalidFormat(_)) => {}
        _ => panic!("Expected InvalidFormat error"),
    }
}

#[test]
fn test_tls_config_with_client_ca() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"
    port = 8443

    [server.tls]
    server_certificate_bundle = "server.pem"
    server_private_key = "server.key"
    client_ca_bundle = "ca.pem"
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    match config.server {
        ServerConfig::Tls(tls_config) => {
            assert_eq!(
                tls_config.tls.server_certificate_bundle,
                PathBuf::from("server.pem")
            );
            assert_eq!(
                tls_config.tls.server_private_key,
                PathBuf::from("server.key")
            );
            assert_eq!(
                tls_config.tls.client_ca_bundle,
                Some(PathBuf::from("ca.pem"))
            );
            assert_eq!(tls_config.tls.client_auth, ClientAuth::Optional);
        }
        ServerConfig::Insecure(_) => panic!("Expected TLS server config"),
    }
}

#[test]
fn test_load_from_file() {
    let config_content = r#"
    [server]
    bind_address = "127.0.0.1"
    port = 8080

    [global]
    max_concurrent_requests = 8

    [blob_store.fs]
    root_dir = "/data/registry"
    "#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(config_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let result = Configuration::load_all(&[temp_file.path()]);
    assert!(result.is_ok());

    let config = result.unwrap();
    assert_eq!(
        config.global.max_concurrent_requests,
        NonZeroUsize::new(8).unwrap()
    );

    match config.server {
        ServerConfig::Insecure(server_config) => {
            assert_eq!(server_config.bind_address.to_string(), "127.0.0.1");
            assert_eq!(server_config.port, 8080);
        }
        ServerConfig::Tls(_) => panic!("Expected Insecure server config"),
    }
}

#[test]
fn test_load_from_nonexistent_file() {
    let result = Configuration::load_all(&["/nonexistent/path/to/config.toml"]);
    assert!(result.is_err());

    match result {
        Err(Error::NotReadable(msg)) => {
            assert!(msg.contains("Unable to read configuration file"));
        }
        _ => panic!("Expected NotReadable error"),
    }
}

// No storage backend gates the durable queue at config time; the startup
// claim-support probe enforces the real precondition.
#[test]
fn job_queue_accepted_on_s3() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.job_queue]

    [blob_store.s3]
    bucket = "blob-bucket"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
    access_key_id = "key"
    secret_key = "secret"
    "#;

    assert!(
        Configuration::load_from_str(config).is_ok(),
        "durable queue on S3 storage must be accepted"
    );
}

#[test]
fn job_queue_accepted_on_fs() {
    let config = r#"
    [server]
    bind_address = "0.0.0.0"

    [global.job_queue]

    [blob_store.fs]
    root_dir = "/data/blobs"
    "#;

    assert!(
        Configuration::load_from_str(config).is_ok(),
        "durable queue on FS storage must be accepted"
    );
}

#[test]
fn event_webhook_valid_global_reference_loads() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [global]
    event_webhooks = ["my-hook"]

    [event_webhook.my-hook]
    url = "https://example.com/hook"
    policy = "optional"
    events = ["manifest.push"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.event_webhook.contains_key("my-hook"));
}

#[test]
fn event_webhook_valid_repo_reference_loads() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [event_webhook.repo-hook]
    url = "https://example.com/hook"
    policy = "optional"
    events = ["manifest.push"]

    [repository.myrepo]
    event_webhooks = ["repo-hook"]
    "#;

    let config = Configuration::load_from_str(config).unwrap();
    assert!(config.repository.contains_key("myrepo"));
    assert!(config.event_webhook.contains_key("repo-hook"));
}

#[test]
fn event_webhook_empty_events_list_fails_load() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [event_webhook.silent]
    url = "https://example.com/hook"
    policy = "required"
    events = []
    "#;

    let result = Configuration::load_from_str(config);
    let err = result.expect_err("event webhook with empty events list must fail to load");
    assert!(
        err.to_string().contains("silent"),
        "Error must identify the offending webhook by name: {err}"
    );
}

/// A `[server.tls]` section that does not parse must fail the load: falling
/// back to plaintext would silently downgrade a registry asked to serve TLS.
#[test]
fn incomplete_tls_section_is_rejected_rather_than_downgraded() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [server.tls]
    server_certificate_bundle = "server.pem"
    "#;

    let result = Configuration::load_from_str(config);

    assert!(
        matches!(result, Err(Error::InvalidFormat(_))),
        "a partial TLS section must be an error, got {result:?}"
    );
}

/// The `client_auth` cross-check is only reachable if a failed TLS parse is an
/// error rather than a fallback.
#[test]
fn required_client_auth_without_a_ca_bundle_is_rejected() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"

    [server.tls]
    server_certificate_bundle = "server.pem"
    server_private_key = "server.key"
    client_auth = "required"
    "#;

    let result = Configuration::load_from_str(config);

    let Err(Error::InvalidFormat(message)) = result else {
        panic!("expected a format error, got {result:?}");
    };
    assert!(
        message.contains("client_ca_bundle"),
        "the error must name the missing key: {message}"
    );
}

/// A server section with no TLS keys is still a plaintext listener.
#[test]
fn a_server_section_without_tls_stays_insecure() {
    let config = r#"
    [blob_store.fs]
    root_dir = "/tmp/test"

    [server]
    bind_address = "0.0.0.0"
    port = 8000
    "#;

    let config = Configuration::load_from_str(config).unwrap();

    assert!(
        matches!(config.server, ServerConfig::Insecure(_)),
        "no TLS keys means a plaintext listener"
    );
}
