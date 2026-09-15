use std::{str::FromStr, sync::Arc};

use serde_json::json;
use tracing::Level;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use angos_oci::request::BlobMount;
use angos_oci::{Digest, Namespace, Reference, Tag};

use crate::auth::authorizer::*;
use crate::{
    auth::Error as AuthError,
    command::bootstrap,
    configuration::Configuration,
    configuration::RegexPattern,
    identity::{AuthMethod, ClientCertificate, ManifestPutTarget, OidcClaims},
    registry::{
        RegistryConfig, Repository,
        metadata_store::MetadataStore,
        repository_resolver::RepositoryResolver,
        test_utils::{for_each_backend, put_blob_direct, test_job_store},
    },
    test_fixtures::{
        configuration::{load_config, minimal_config, try_load_config},
        logging::LogCapture,
        requests::parts_with_uri,
    },
};

#[test]
fn test_authorizer_new_minimal() {
    let config = minimal_config();
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let authorizer = Authorizer::new(&config, &cache);

    assert!(authorizer.is_ok());
    let authorizer = authorizer.unwrap();
    assert!(authorizer.global_authorization_webhook.is_none());
    assert!(authorizer.repositories.is_empty());
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

    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let authorizer = Authorizer::new(&config, &cache);

    assert!(authorizer.is_ok());
    let authorizer = authorizer.unwrap();
    assert_eq!(authorizer.repositories.len(), 1);
    assert!(authorizer.repositories.contains_key("myrepo"));
}

#[test]
fn empty_repository_webhook_reference_builds_without_a_webhook() {
    // A blank `authorization_webhook` is unset, matching config validation, so
    // construction resolves it to no repository webhook rather than failing the
    // authorizer lookup for an empty name.
    let config = load_config(
        r#"
            [repository.myrepo]
            namespace_pattern = "^myrepo/.*"
            authorization_webhook = ""
        "#,
    );
    let cache = angos_cache::Config::Memory.to_backend().unwrap();

    let authorizer = Authorizer::new(&config, &cache).expect("blank webhook ref must build");
    let repo = authorizer
        .repositories
        .get("myrepo")
        .expect("repository present");
    assert!(repo.authorization_webhook.is_none());
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
            claims: HashMap::from([
                ("sub".to_string(), json!("repo:private/repo:ref:main")),
                ("email".to_string(), json!("person@example.com")),
                ("custom_claim".to_string(), json!("internal-secret")),
            ]),
        }),
        client_ip: Some("192.0.2.10".to_string()),
        auth_method: AuthMethod::Mtls,
        from_registry_token: false,
    };

    tracing::subscriber::with_default(subscriber, || log_denial("test reason", &identity));

    let logs = log_capture.contents();
    assert!(logs.contains("test reason"), "logs were: {logs}");
    assert!(logs.contains("mtls"), "logs were: {logs}");
    assert!(logs.contains("client-123"), "logs were: {logs}");
    assert!(logs.contains("ci-bot"), "logs were: {logs}");
    assert!(logs.contains("192.0.2.10"), "logs were: {logs}");
    assert!(logs.contains("BuildOrg"), "logs were: {logs}");
    assert!(logs.contains("build-cert"), "logs were: {logs}");
    assert!(logs.contains("github-actions"), "logs were: {logs}");
    assert!(!logs.contains("person@example.com"), "logs were: {logs}");
    assert!(!logs.contains("repo:private/repo"), "logs were: {logs}");
    assert!(!logs.contains("internal-secret"), "logs were: {logs}");
    assert!(!logs.contains("custom_claim"), "logs were: {logs}");
    assert!(!logs.contains("email"), "logs were: {logs}");
    assert!(!logs.contains("sub"), "logs were: {logs}");
}

/// `has_repository_policy()` lets a global rule hand a request over to the
/// repository that declares its own rules, and keeps the global decision for a
/// namespace no such repository covers.
#[tokio::test]
async fn has_repository_policy_hands_the_decision_to_the_repository() {
    let config = load_config(
        r#"
            [global.access_policy]
            default = "deny"
            rules = ["has_repository_policy()"]

            [repository.guarded.access_policy]
            default = "deny"
            rules = ["identity.username == 'alice'"]

            [repository.unguarded]
        "#,
    );
    let (authorizer, registry) = authorizer_and_registry(&config).await;
    let parts = parts_with_uri("/v2/");
    let identity = |name: &str| ClientIdentity {
        username: Some(name.to_string()),
        ..ClientIdentity::new(None)
    };
    let pull = |namespace: &str| Action::GetManifest {
        namespace: Namespace::new(namespace).unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    };
    let authorize = async |action, identity| {
        authorizer
            .authorize_request(&action, &identity, &parts, &registry)
            .await
    };

    authorize(pull("guarded/app"), identity("alice"))
        .await
        .expect("the repository policy decides once the global rule defers to it");

    for (action, identity, reason) in [
        (
            pull("guarded/app"),
            identity("bob"),
            "the repository policy still denies whoever its own rules exclude",
        ),
        (
            pull("unguarded/app"),
            identity("alice"),
            "a repository declaring no policy has nothing to defer to",
        ),
        (
            pull("undeclared/app"),
            identity("alice"),
            "a namespace no repository declares has nothing to defer to",
        ),
    ] {
        let result = authorize(action, identity).await;
        assert!(
            matches!(result, Err(AuthError::Unauthorized(_))),
            "{reason}"
        );
    }
}

/// The catalog hides a repository whose per-repository policy denies the
/// caller, and shows one it allows; the global policy alone gates the rest.
#[tokio::test]
async fn catalog_filtering_honours_the_repository_policy() {
    let config = load_config(
        r#"
            [global.access_policy]
            default = "deny"
            rules = ["request.action == 'list-tags'"]

            [repository.guarded.access_policy]
            default = "deny"
            rules = ["identity.username == 'alice'"]

            [repository.unguarded]
        "#,
    );
    let (authorizer, registry) = authorizer_and_registry(&config).await;
    let identity = |name: &str| ClientIdentity {
        username: Some(name.to_string()),
        ..ClientIdentity::new(None)
    };
    let listable = |ns: &str, name: &str| {
        authorizer.allows_catalog_entry(&Namespace::new(ns).unwrap(), &identity(name), &registry)
    };

    assert!(
        listable("guarded/app", "alice"),
        "the repository policy admits alice"
    );
    assert!(
        !listable("guarded/app", "bob"),
        "the repository policy hides the entry from bob"
    );
    assert!(
        listable("unguarded/app", "bob"),
        "a repository declaring no policy falls to the global list-tags rule"
    );
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

async fn create_pull_through_registry(config: &Configuration) -> Arc<Registry> {
    let blob_backend = Arc::new(config.blob_store.build_backend().unwrap());
    let auth_cache = config.cache.to_backend().unwrap();
    let storage_config = config.resolve_registry_storage();
    let handles = bootstrap::build_object_store(&storage_config).unwrap();
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

    let registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        enable_blob_redirect: config.global.enable_blob_redirect,
        enable_manifest_redirect: config.global.enable_manifest_redirect,
        max_manifest_size_bytes: config.global.max_manifest_size_bytes(),
        global_immutable_tags: config.global.immutable_tags,
        global_immutable_tags_exclusions: config.global.immutable_tags_exclusions.clone(),
        ..RegistryConfig::new(test_job_store(&metadata_store))
    };

    Registry::new(blob_backend, metadata_store, resolver, registry_config)
}

/// Builds the authorizer and registry pair every `authorize_request` test needs.
async fn authorizer_and_registry(config: &Configuration) -> (Authorizer, Arc<Registry>) {
    let cache = angos_cache::Config::Memory.to_backend().unwrap();
    let authorizer = Authorizer::new(config, &cache).unwrap();
    let registry = create_pull_through_registry(config).await;
    (authorizer, registry)
}

#[tokio::test]
async fn test_pull_through_repo_allows_delete_manifest() {
    let config = create_pull_through_config();
    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let namespace = Namespace::new("docker-io/library/nginx").unwrap();
    let reference = Reference::from_str("latest").unwrap();
    let route = Action::DeleteManifest {
        namespace,
        reference,
    };
    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

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
    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let namespace = Namespace::new("docker-io/library/nginx").unwrap();
    let digest =
        Digest::from_str("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();
    let route = Action::DeleteBlob { namespace, digest };
    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

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
    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

    let namespace = Namespace::new("docker-io/library/nginx").unwrap();
    let put_manifest_route = Action::PutManifest {
        namespace: namespace.clone(),
        target: ManifestPutTarget::Tag(Tag::new("latest").unwrap()),
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
        digest_algorithm: None,
    };
    let result = authorizer
        .authorize_request(&start_upload_route, &identity, &parts, &registry)
        .await;
    assert!(
        result.is_err(),
        "StartUpload should be blocked on pull-through cache repositories"
    );
}

// A pull-through push is rejected before the webhook is consulted: the cache
// never accepts writes, so the paid webhook round-trip is skipped. The mock's
// `expect(0)` fails on drop if the webhook is ever called.
#[tokio::test]
async fn pull_through_push_rejected_before_calling_webhook() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock_server)
        .await;

    let config = load_config(&format!(
        r#"
            [global.access_policy]
            default = "allow"

            [repository."docker-io"]
            authorization_webhook = "gatekeeper"

            [[repository."docker-io".upstream]]
            url = "https://registry-1.docker.io"

            [auth.webhook.gatekeeper]
            url = "{url}"
            timeout_ms = 1000
        "#,
        url = mock_server.uri()
    ));
    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");
    let push = Action::StartUpload {
        namespace: Namespace::new("docker-io/library/nginx").unwrap(),
        digest: None,
        digest_algorithm: None,
    };

    let result = authorizer
        .authorize_request(&push, &identity, &parts, &registry)
        .await;
    assert!(
        matches!(result, Err(AuthError::Unauthorized(_))),
        "pull-through push must be denied, got: {result:?}"
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

    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

    let result = authorizer
        .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
        .await;

    assert!(
        matches!(result, Err(AuthError::Unauthorized(_))),
        "global deny policy must reject ApiVersion, got: {result:?}"
    );
}

// An explicit repository policy of `default = "deny"` with no rules must deny
// every namespaced request; only an absent policy table means "no repository
// policy", never a deny-all sentinel.
#[tokio::test]
async fn repository_deny_all_policy_rejects_namespaced_request() {
    let config = load_config(
        r#"
            [global.access_policy]
            default = "allow"

            [repository.myrepo.access_policy]
            default = "deny"
        "#,
    );

    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let route = Action::GetManifest {
        namespace: Namespace::new("myrepo/app").unwrap(),
        reference: Reference::from_str("latest").unwrap(),
    };
    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

    let result = authorizer
        .authorize_request(&route, &identity, &parts, &registry)
        .await;

    assert!(
        matches!(result, Err(AuthError::Unauthorized(_))),
        "an explicit deny-all repository policy must deny, got: {result:?}"
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

    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

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

    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

    let result = authorizer
        .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
        .await;

    assert!(
        matches!(result, Err(AuthError::Unauthorized(_))),
        "global webhook returning 403 must deny the request, got: {result:?}"
    );
}

// A namespace no `[repository]` declares has no repository webhook, so the
// global one gates it rather than being skipped for the very namespaces no
// repository policy covers.
#[tokio::test]
async fn global_webhook_gates_a_namespace_no_repository_declares() {
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

    let (authorizer, registry) = authorizer_and_registry(&config).await;
    let action = Action::GetManifest {
        namespace: Namespace::new("no-such-repo/image").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    };

    let result = authorizer
        .authorize_request(
            &action,
            &ClientIdentity::new(None),
            &parts_with_uri("/v2/"),
            &registry,
        )
        .await;

    assert!(
        matches!(result, Err(AuthError::Unauthorized(_))),
        "the global webhook must decide a namespace no repository declares, got: {result:?}"
    );
}

// Invalid-regex unreachability.
//
// `RegexPattern` compiles the regex at TOML deserialise time. An invalid
// pattern causes `Configuration::load_from_str` to return `Err` before any
// `Authorizer` is constructed. This test documents the `RegexPattern::compile`
// API directly so the compile-time rejection invariant is grep-able from here.
#[test]
fn regex_pattern_compile_rejects_invalid_pattern() {
    let err = RegexPattern::compile("[invalid");
    assert!(
        err.is_err(),
        "an invalid regex must be rejected by RegexPattern::compile"
    );
}

// A namespace that maps to no configured repository is passed through
// without error or panic under an allow policy.
//
// `authorize_request` skips the repository layer when
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

    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

    let action = Action::GetManifest {
        namespace: Namespace::new("no-such-repo/image").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
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

    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

    let action = Action::GetManifest {
        namespace: Namespace::new("myrepo/app").unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
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
    let config = load_config(
        r#"
            [global.access_policy]
            default = "allow"
            rules = ["nonexistent_var"]
        "#,
    );

    let (authorizer, registry) = authorizer_and_registry(&config).await;

    let identity = ClientIdentity::new(None);
    let parts = parts_with_uri("/v2/");

    let result = authorizer
        .authorize_request(&Action::ApiVersion, &identity, &parts, &registry)
        .await;

    assert!(
        matches!(result, Err(AuthError::Unauthorized(_))),
        "an indeterminate global policy must deny the request, got: {result:?}"
    );
}

#[tokio::test]
async fn authorize_mount_source_requires_read_on_the_source() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let source = &Namespace::new("test-repo/source").unwrap();
        let content = b"mount authorization blob";

        let metadata_store = test_case.metadata_store();
        let digest = put_blob_direct(metadata_store.object_store(), content).await;
        registry
            .metadata_store()
            .grant(source, &digest)
            .await
            .unwrap();

        let config = load_config(
            r#"
                [global.access_policy]
                default = "deny"
                rules = ["identity.id == 'reader'"]

                [repository."test-repo".access_policy]
                default = "allow"
            "#,
        );
        let cache = angos_cache::Config::Memory.to_backend().unwrap();
        let authorizer = Authorizer::new(&config, &cache).unwrap();

        let parts = parts_with_uri("/v2/");
        let mut reader = ClientIdentity::new(None);
        reader.id = Some("reader".to_string());
        let stranger = ClientIdentity::new(None);

        for from in [Some(source.clone()), None] {
            let mount = BlobMount {
                digest: digest.clone(),
                from,
            };
            assert!(
                authorizer
                    .authorize_mount_source(&mount, &reader, &parts, registry)
                    .await
                    .unwrap()
                    .is_some(),
                "a caller permitted to read the source must be allowed to mount"
            );
            assert!(
                authorizer
                    .authorize_mount_source(&mount, &stranger, &parts, registry)
                    .await
                    .unwrap()
                    .is_none(),
                "a caller denied read on the source must not be allowed to mount"
            );
        }
    })
    .await;
}
