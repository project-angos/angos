use std::{
    io::{self, Write},
    str::FromStr,
    sync::{Arc, Mutex},
};

use serde_json::json;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::*;
use crate::{
    cache,
    command::server::Error as ServerError,
    configuration::Configuration,
    identity::{ClientCertificate, OidcClaims},
    oci::{Digest, Namespace, Reference},
    registry::{
        RegistryConfig, Repository, metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
    },
    test_fixtures::configuration::{load_config, minimal_config, try_load_config},
};

#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl LogCapture {
    fn contents(&self) -> String {
        let bytes = self.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogCapture {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(Arc::clone(&self.0))
    }
}

fn create_minimal_config() -> Configuration {
    minimal_config()
}

#[test]
fn test_authorizer_new_minimal() {
    let config = create_minimal_config();
    let cache = cache::Config::Memory.to_backend().unwrap();

    let authorizer = Authorizer::new(&config, &cache);

    assert!(authorizer.is_ok());
    let authorizer = authorizer.unwrap();
    assert!(authorizer.global_authorization_webhook.is_none());
    assert!(!authorizer.global_immutable_tags);
    assert!(authorizer.global_immutable_tags_exclusions.is_empty());
    assert!(authorizer.repositories.is_empty());
}

#[test]
fn test_authorizer_new_with_global_access_policy() {
    let config = load_config(
        r#"
            [global.access_policy]
            default = "allow"
            rules = ["identity.username == 'admin'"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();

    let authorizer = Authorizer::new(&config, &cache);

    assert!(authorizer.is_ok());
}

#[test]
fn test_authorizer_new_with_global_immutable_tags() {
    let config = load_config(
        r#"
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$", "^dev-.*"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();

    let authorizer = Authorizer::new(&config, &cache);

    assert!(authorizer.is_ok());
    let authorizer = authorizer.unwrap();
    assert!(authorizer.global_immutable_tags);
    assert_eq!(authorizer.global_immutable_tags_exclusions.len(), 2);
}

#[test]
fn test_authorizer_new_with_repository_config() {
    let config = load_config(
        r#"
            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["^test-.*"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();

    let authorizer = Authorizer::new(&config, &cache);

    assert!(authorizer.is_ok());
    let authorizer = authorizer.unwrap();
    assert_eq!(authorizer.repositories.len(), 1);
    assert!(authorizer.repositories.contains_key("myrepo"));
}

#[test]
fn test_invalid_global_regex_fails_at_deserialize() {
    let result = try_load_config(
        r#"
            immutable_tags = true
            immutable_tags_exclusions = ["[invalid"]
        "#,
    );

    assert!(
        result.is_err(),
        "invalid global regex must fail at deserialize time"
    );
}

#[test]
fn test_invalid_repository_regex_fails_at_deserialize() {
    let result = try_load_config(
        r#"
            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["[invalid"]
        "#,
    );

    assert!(
        result.is_err(),
        "invalid repository regex must fail at deserialize time"
    );
}

#[test]
fn test_is_tag_mutable_with_global_setting() {
    let config = load_config(
        r"
            immutable_tags = true
        ",
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(!authorizer.is_tag_mutable(None, "v1.0.0"));
}

#[test]
fn test_is_tag_mutable_with_global_exclusions() {
    let config = load_config(
        r#"
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$", "^dev-.*"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(authorizer.is_tag_mutable(None, "latest"));
    assert!(authorizer.is_tag_mutable(None, "dev-branch"));
    assert!(!authorizer.is_tag_mutable(None, "v1.0.0"));
}

#[test]
fn test_is_tag_mutable_with_repository_setting() {
    let config = load_config(
        r#"
            immutable_tags = false

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(!authorizer.is_tag_mutable(Some("myrepo"), "v1.0.0"));
}

#[test]
fn test_is_tag_mutable_with_repository_exclusions() {
    let config = load_config(
        r#"
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$"]

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["^test-.*"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(authorizer.is_tag_mutable(Some("myrepo"), "test-123"));
    assert!(!authorizer.is_tag_mutable(Some("myrepo"), "latest"));
    assert!(!authorizer.is_tag_mutable(Some("myrepo"), "v1.0.0"));
}

#[test]
fn test_is_tag_mutable_with_repository_name() {
    let config = load_config(
        r#"
            immutable_tags = false

            [repository."docker-io"]
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(!authorizer.is_tag_mutable(Some("docker-io"), "v1.0.0"));
    assert!(authorizer.is_tag_mutable(Some("docker-io"), "latest"));
    assert!(authorizer.is_tag_mutable(None, "v1.0.0"));
}

#[test]
fn test_is_tag_mutable_when_not_immutable() {
    let config = load_config(
        r#"
            immutable_tags = false

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = false
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(authorizer.is_tag_mutable(Some("myrepo"), "any-tag"));
}

// When immutable_tags is true and the tag matches an exclusion pattern, the
// tag is mutable (the exclusion carves out a writable subset).
#[test]
fn is_tag_mutable_returns_true_when_immutable_but_excluded() {
    let config = load_config(
        r#"
            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$", "^dev-.*"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(
        authorizer.is_tag_mutable(Some("myrepo"), "latest"),
        "'latest' must be mutable because it matches the exclusion pattern"
    );
    assert!(
        authorizer.is_tag_mutable(Some("myrepo"), "dev-feature"),
        "'dev-feature' must be mutable because it matches 'dev-.*'"
    );
}

// When immutable_tags is true and the tag does not match any exclusion,
// the tag is immutable.
#[test]
fn is_tag_mutable_returns_false_when_immutable_and_not_excluded() {
    let config = load_config(
        r#"
            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            immutable_tags = true
            immutable_tags_exclusions = ["^latest$"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    assert!(
        !authorizer.is_tag_mutable(Some("myrepo"), "v1.0.0"),
        "'v1.0.0' must be immutable: immutable_tags=true and not excluded"
    );
}

#[test]
fn test_check_immutable_tag_returns_conflict_for_tagged_putmanifest() {
    let config = load_config(
        r#"
            immutable_tags = true

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();

    let action = Action::PutManifest {
        namespace: Namespace::new("myrepo/app").unwrap(),
        reference: Reference::from_str("v1.0.0").unwrap(),
    };

    let result = authorizer.check_immutable_tag("myrepo", &action);

    let Err(ServerError::Conflict(msg)) = result else {
        panic!("expected Err(ServerError::Conflict(_)), got: {result:?}");
    };
    assert!(
        msg.contains("v1.0.0") && msg.contains("immutable"),
        "error message must mention the tag and 'immutable', got: {msg}"
    );
}

#[test]
fn log_denial_uses_audit_identity_without_oidc_claims() {
    let log_capture = LogCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_writer(log_capture.clone())
        .with_ansi(false)
        .finish();
    let identity = ClientIdentity {
        id: Some("client-123".to_string()),
        username: Some("ci-bot".to_string()),
        certificate: ClientCertificate {
            organizations: vec!["BuildOrg".to_string()],
            common_names: vec!["build-cert".to_string()],
        },
        oidc: Some(OidcClaims {
            provider_name: "github-actions".to_string(),
            provider_type: "GitHub Actions".to_string(),
            claims: HashMap::from([
                ("sub".to_string(), json!("repo:private/repo:ref:main")),
                ("email".to_string(), json!("person@example.com")),
                ("custom_claim".to_string(), json!("internal-secret")),
            ]),
        }),
        client_ip: Some("192.0.2.10".to_string()),
    };

    tracing::subscriber::with_default(subscriber, || log_denial("test reason", &identity));

    let logs = log_capture.contents();
    assert!(logs.contains("test reason"), "logs were: {logs}");
    assert!(logs.contains("multiple"), "logs were: {logs}");
    assert!(logs.contains("client-123"), "logs were: {logs}");
    assert!(logs.contains("ci-bot"), "logs were: {logs}");
    assert!(logs.contains("192.0.2.10"), "logs were: {logs}");
    assert!(logs.contains("BuildOrg"), "logs were: {logs}");
    assert!(logs.contains("build-cert"), "logs were: {logs}");
    assert!(logs.contains("github-actions"), "logs were: {logs}");
    assert!(logs.contains("GitHub Actions"), "logs were: {logs}");
    assert!(!logs.contains("person@example.com"), "logs were: {logs}");
    assert!(!logs.contains("repo:private/repo"), "logs were: {logs}");
    assert!(!logs.contains("internal-secret"), "logs were: {logs}");
    assert!(!logs.contains("custom_claim"), "logs were: {logs}");
    assert!(!logs.contains("email"), "logs were: {logs}");
    assert!(!logs.contains("sub"), "logs were: {logs}");
}

fn create_pull_through_config() -> Configuration {
    load_config(
        r#"
            [global.access_policy]
            default = "allow"

            [repository."docker-io"]

            [[repository."docker-io".upstream]]
            url = "https://registry-1.docker.io"
        "#,
    )
}

async fn create_pull_through_registry(config: &Configuration) -> Registry {
    let blob_backend = std::sync::Arc::new(config.blob_store.build_backend().unwrap());
    let auth_cache = config.cache.to_backend().unwrap();
    let storage_config = config.resolve_registry_storage();
    let handles = storage_config.build_store().await.unwrap();
    let metadata_store = Arc::new(MetadataStore::builder(handles).build());

    let mut repositories_map = HashMap::new();
    for (name, repo_config) in &config.repository {
        let repo = Repository::new(
            name,
            repo_config,
            &auth_cache,
            config.global.max_manifest_size_bytes(),
        )
        .await
        .unwrap();
        repositories_map.insert(name.clone(), repo);
    }
    let resolver = Arc::new(
        RepositoryResolver::new(Arc::new(repositories_map))
            .expect("test repositories must not have overlapping prefixes"),
    );

    let registry_config = RegistryConfig::default()
        .update_pull_time(config.global.update_pull_time)
        .enable_blob_redirect(config.global.resolved_enable_blob_redirect())
        .enable_manifest_redirect(config.global.resolved_enable_manifest_redirect())
        .max_manifest_size_bytes(config.global.max_manifest_size_bytes())
        .global_immutable_tags(config.global.immutable_tags)
        .global_immutable_tags_exclusions(config.global.immutable_tags_exclusions.clone());

    Registry::new(blob_backend, metadata_store, resolver, registry_config).unwrap()
}

#[tokio::test]
async fn test_pull_through_repo_allows_delete_manifest() {
    let config = create_pull_through_config();
    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let namespace = Namespace::new("docker-io/library/nginx").unwrap();
    let reference = Reference::from_str("latest").unwrap();
    let route = Action::DeleteManifest {
        namespace,
        reference,
    };
    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let result = authorizer
        .authorize_request(&route, &identity, &parts, &registry)
        .await;

    assert!(
        result.is_ok(),
        "DeleteManifest should be allowed on pull-through cache repositories, got: {result:?}"
    );
}

#[tokio::test]
async fn test_pull_through_repo_allows_delete_blob() {
    let config = create_pull_through_config();
    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let namespace = Namespace::new("docker-io/library/nginx").unwrap();
    let digest =
        Digest::from_str("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();
    let route = Action::DeleteBlob { namespace, digest };
    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let result = authorizer
        .authorize_request(&route, &identity, &parts, &registry)
        .await;

    assert!(
        result.is_ok(),
        "DeleteBlob should be allowed on pull-through cache repositories, got: {result:?}"
    );
}

#[tokio::test]
async fn test_pull_through_repo_blocks_push_operations() {
    let config = create_pull_through_config();
    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let namespace = Namespace::new("docker-io/library/nginx").unwrap();
    let reference = Reference::from_str("latest").unwrap();
    let put_manifest_route = Action::PutManifest {
        namespace: namespace.clone(),
        reference,
    };
    let result = authorizer
        .authorize_request(&put_manifest_route, &identity, &parts, &registry)
        .await;
    assert!(
        result.is_err(),
        "PutManifest should be blocked on pull-through cache repositories"
    );

    let start_upload_route = Action::StartUpload {
        namespace,
        digest: None,
    };
    let result = authorizer
        .authorize_request(&start_upload_route, &identity, &parts, &registry)
        .await;
    assert!(
        result.is_err(),
        "StartUpload should be blocked on pull-through cache repositories"
    );
}

// Global deny policy rejects every request regardless of action.
//
// The `[global.access_policy]` block with `default = "deny"` and no allow-rules
// causes `AccessPolicy::evaluate` to return `PolicyDecision::Deny` for all identities.
// `authorize_request` must short-circuit before consulting any webhook or repository.
#[tokio::test]
async fn global_deny_policy_rejects_all_requests() {
    let config = load_config(
        r#"
            [global.access_policy]
            default = "deny"
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let result = authorizer
        .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
        .await;

    assert!(
        matches!(result, Err(ServerError::Unauthorized(_))),
        "global deny policy must reject ApiVersion, got: {result:?}"
    );
}

// Global allow policy + global webhook returning 200 → request allowed.
//
// `Action::ApiVersion` carries no namespace, so `authorize_request` takes the
// non-namespace branch and calls the global webhook directly.  A 200 response
// means the webhook grants access.
#[tokio::test]
async fn global_webhook_path_allow_when_webhook_returns_200() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
            authorization_webhook = "gatekeeper"

            [global.access_policy]
            default = "allow"

            [auth.webhook.gatekeeper]
            url = "{url}"
            timeout_ms = 1000
            "#,
        url = mock_server.uri()
    ));

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let result = authorizer
        .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
        .await;

    assert!(
        result.is_ok(),
        "global webhook returning 200 must allow the request, got: {result:?}"
    );
}

// Global allow policy + global webhook returning 403 → request denied.
#[tokio::test]
async fn global_webhook_path_deny_when_webhook_returns_403() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
            authorization_webhook = "gatekeeper"

            [global.access_policy]
            default = "allow"

            [auth.webhook.gatekeeper]
            url = "{url}"
            timeout_ms = 1000
            "#,
        url = mock_server.uri()
    ));

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let result = authorizer
        .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
        .await;

    assert!(
        matches!(result, Err(ServerError::Unauthorized(_))),
        "global webhook returning 403 must deny the request, got: {result:?}"
    );
}

// Invalid-regex unreachability.
//
// `RegexPattern` compiles the regex at TOML deserialise time. An invalid
// pattern causes `Configuration::load_from_str` to return `Err` before any
// `Authorizer` is constructed, so it can never reach `is_tag_mutable`. This
// test documents the `RegexPattern::compile` API directly so the
// compile-time rejection invariant is grep-able from here.
#[test]
fn regex_pattern_compile_rejects_invalid_pattern() {
    let err = RegexPattern::compile("[invalid");
    assert!(
        err.is_err(),
        "an invalid regex must be rejected by RegexPattern::compile; \
             it can therefore never reach is_tag_mutable"
    );
}

// A namespace that maps to no configured repository is passed through
// without error or panic under an allow policy.
//
// `authorize_namespace_request` returns `Ok(())` early when
// `get_repository_for_namespace` returns `Err`, so the default policy is
// applied implicitly (allow here).
#[tokio::test]
async fn authorize_request_unknown_namespace_is_allowed_under_allow_policy() {
    let config = load_config(
        r#"
            [global.access_policy]
            default = "allow"
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let action = Action::GetManifest {
        namespace: Namespace::new("no-such-repo/image").unwrap(),
        reference: Reference::Tag("latest".to_string()),
    };

    let result = authorizer
        .authorize_request(&action, &identity, &parts, &registry)
        .await;

    assert!(
        result.is_ok(),
        "a namespace that matches no repository must not panic or deny under an allow policy, got: {result:?}"
    );
}

// Webhook unreachable → fail-closed.
//
// When the webhook endpoint is unreachable, `WebhookAuthorizer::authorize`
// returns `Err(Error::Unauthorized(...))`.  `authorize_namespace_request`
// propagates that error so the authorizer returns `Err`, not `Ok(false)`.
// This distinguishes a transport failure from an explicit deny on dashboards.
#[tokio::test]
async fn webhook_unreachable_fails_closed() {
    let config = load_config(
        r#"
            [global.access_policy]
            default = "allow"

            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            authorization_webhook = "gatekeeper"

            [auth.webhook.gatekeeper]
            url = "http://127.0.0.1:1"
            timeout_ms = 500
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let action = Action::GetManifest {
        namespace: Namespace::new("myrepo/app").unwrap(),
        reference: Reference::Tag("latest".to_string()),
    };

    let result = authorizer
        .authorize_request(&action, &identity, &parts, &registry)
        .await;

    assert!(
        result.is_err(),
        "an unreachable webhook must produce Err (fail-closed), not Ok; got: {result:?}"
    );
}

// A global allow-mode policy whose DENY rule throws at runtime must deny the
// request.  Previously the evaluation error was swallowed and the default
// allow was returned.
#[tokio::test]
async fn indeterminate_global_policy_denies_request() {
    use crate::command::server::Error as ServerError;

    let config = load_config(
        r#"
            [global.access_policy]
            default = "allow"
            rules = ["nonexistent_var"]
        "#,
    );

    let cache = cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(&config, &cache).unwrap();
    let registry = create_pull_through_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let (parts, ()) = hyper::Request::builder()
        .uri("/v2/")
        .body(())
        .unwrap()
        .into_parts();

    let result = authorizer
        .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
        .await;

    assert!(
        matches!(result, Err(ServerError::Unauthorized(_))),
        "an indeterminate global policy must deny the request, got: {result:?}"
    );
}
