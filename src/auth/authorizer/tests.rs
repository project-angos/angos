use std::str::FromStr;
use std::sync::Arc;

use serde_json::json;
use tracing::Level;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use angos_oci::request::{BlobMount, ManifestPutTarget};
use angos_oci::{Digest, Namespace, Reference, Tag};

use super::*;
use crate::auth::Error as AuthError;
use crate::command::bootstrap;
use crate::configuration::Configuration;
use crate::identity::{AuthMethod, ClientCertificate, OidcClaims};
use crate::registry::metadata_store::{MetadataStore, Settings};
use crate::registry::repository_resolver::RepositoryResolver;
use crate::registry::test_utils::{for_each_backend, put_blob_direct, test_job_store};
use crate::registry::{RegistryConfig, Repository};
use crate::test_fixtures::configuration::load_config;
use crate::test_fixtures::logging::LogCapture;
use crate::test_fixtures::requests::parts_with_uri;

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
    let authorizer = Authorizer::new(&config).expect("blank webhook ref must build");
    let repo = authorizer
        .repositories
        .get("myrepo")
        .expect("repository present");
    assert!(repo.authorization_webhook.is_none());
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
    for audited in [
        "test reason",
        "mtls",
        "client-123",
        "ci-bot",
        "192.0.2.10",
        "BuildOrg",
        "build-cert",
        "github-actions",
    ] {
        assert!(logs.contains(audited), "{audited} missing from: {logs}");
    }
    for claim in [
        "person@example.com",
        "repo:private/repo",
        "internal-secret",
        "custom_claim",
        "email",
        "sub",
    ] {
        assert!(!logs.contains(claim), "{claim} leaked into: {logs}");
    }
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
    let authorize = async |action, identity| {
        authorizer
            .authorize_request(&action, &identity, &parts, &registry)
            .await
    };

    authorize(get_manifest("guarded/app"), identity("alice"))
        .await
        .expect("the repository policy decides once the global rule defers to it");

    for (action, identity, reason) in [
        (
            get_manifest("guarded/app"),
            identity("bob"),
            "the repository policy still denies whoever its own rules exclude",
        ),
        (
            get_manifest("unguarded/app"),
            identity("alice"),
            "a repository declaring no policy has nothing to defer to",
        ),
        (
            get_manifest("undeclared/app"),
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

const PULL_THROUGH: &str = r#"
    [global.access_policy]
    default = "allow"

    [repository."docker-io"]

    [[repository."docker-io".upstream]]
    url = "https://registry-1.docker.io"
"#;

async fn create_pull_through_registry(config: &Configuration) -> Arc<Registry> {
    let blob_backend = Arc::new(config.blob_store.build_backend().unwrap());
    let auth_cache = config.cache.to_backend().unwrap();
    let storage_config = config.resolve_registry_storage();
    let handles = bootstrap::build_object_store(&storage_config).unwrap();
    let metadata_store = Arc::new(MetadataStore::new(handles, Settings::default()));

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
    let resolver = Arc::new(RepositoryResolver::new(Arc::new(repositories_map)).unwrap());

    let registry_config = RegistryConfig {
        update_pull_time: config.global.update_pull_time,
        redirect: config.global.redirect,
        max_manifest_size_bytes: config.global.max_manifest_size_bytes(),
        global_immutable_tags: config.global.immutable_tags,
        global_immutable_tags_exclusions: config.global.immutable_tags_exclusions.clone(),
        ..RegistryConfig::new(test_job_store(&metadata_store))
    };

    Registry::new(blob_backend, metadata_store, resolver, registry_config)
}

async fn authorizer_and_registry(config: &Configuration) -> (Authorizer, Arc<Registry>) {
    let authorizer = Authorizer::new(config).unwrap();
    let registry = create_pull_through_registry(config).await;
    (authorizer, registry)
}

/// Authorizes `action` for an anonymous client under `config`.
async fn authorize(config: &Configuration, action: &Action) -> Result<(), AuthError> {
    let (authorizer, registry) = authorizer_and_registry(config).await;
    authorizer
        .authorize_request(
            action,
            &ClientIdentity::new(None),
            &parts_with_uri("/v2/"),
            &registry,
        )
        .await
}

fn get_manifest(namespace: &str) -> Action {
    Action::GetManifest {
        namespace: Namespace::new(namespace).unwrap(),
        reference: Reference::Tag(Tag::new("latest").unwrap()),
    }
}

fn start_upload(namespace: &str) -> Action {
    Action::StartUpload {
        namespace: Namespace::new(namespace).unwrap(),
        digest: None,
        digest_algorithm: None,
    }
}

/// A pull-through cache refuses pushes but still allows deletes.
#[tokio::test]
async fn pull_through_repo_blocks_only_pushes() {
    let config = load_config(PULL_THROUGH);
    let namespace = Namespace::new("docker-io/library/nginx").unwrap();
    let digest =
        Digest::from_str("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();

    for (action, allowed) in [
        (
            Action::DeleteManifest {
                namespace: namespace.clone(),
                reference: Reference::from_str("latest").unwrap(),
            },
            true,
        ),
        (
            Action::DeleteBlob {
                namespace: namespace.clone(),
                digest,
            },
            true,
        ),
        (
            Action::PutManifest {
                namespace,
                target: ManifestPutTarget::Tag(Tag::new("latest").unwrap()),
            },
            false,
        ),
        (start_upload("docker-io/library/nginx"), false),
    ] {
        let result = authorize(&config, &action).await;
        assert_eq!(result.is_ok(), allowed, "{action:?}: {result:?}");
    }
}

/// The paid webhook round-trip is skipped for a pull-through push; the mock's
/// `expect(0)` fails on drop if the webhook is called.
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

    let result = authorize(&config, &start_upload("docker-io/library/nginx")).await;
    assert!(
        matches!(result, Err(AuthError::Unauthorized(_))),
        "got: {result:?}"
    );
}

/// An explicit repository `default = "deny"` denies everything (only an absent
/// table means no repository policy), an indeterminate global rule denies, and
/// a namespace no repository covers gets the global decision.
#[tokio::test]
async fn access_policies_decide_requests() {
    let cases = [
        (
            "[global.access_policy]\ndefault = \"deny\"",
            Action::ApiVersion,
            false,
        ),
        (
            "[global.access_policy]\ndefault = \"allow\"\nrules = [\"nonexistent_var\"]",
            Action::ApiVersion,
            false,
        ),
        (
            "[global.access_policy]\ndefault = \"allow\"\n[repository.myrepo.access_policy]\ndefault = \"deny\"",
            get_manifest("myrepo/app"),
            false,
        ),
        (
            "[global.access_policy]\ndefault = \"allow\"",
            get_manifest("no-such-repo/image"),
            true,
        ),
    ];

    for (toml, action, allowed) in cases {
        let result = authorize(&load_config(toml), &action).await;
        if allowed {
            assert!(result.is_ok(), "{toml}: {result:?}");
        } else {
            assert!(
                matches!(result, Err(AuthError::Unauthorized(_))),
                "{toml}: {result:?}"
            );
        }
    }
}

/// The global webhook gates requests without a namespace and namespaces no
/// `[repository]` declares, which have no repository webhook of their own.
#[tokio::test]
async fn global_webhook_decides_requests_outside_any_repository() {
    for (status, action, allowed) in [
        (200, Action::ApiVersion, true),
        (403, Action::ApiVersion, false),
        (403, get_manifest("no-such-repo/image"), false),
    ] {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status))
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

        let result = authorize(&config, &action).await;
        if allowed {
            assert!(result.is_ok(), "{status}: {result:?}");
        } else {
            assert!(
                matches!(result, Err(AuthError::Unauthorized(_))),
                "{status}: {result:?}"
            );
        }
    }
}

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

    let result = authorize(&config, &get_manifest("myrepo/app")).await;
    assert!(result.is_err(), "got: {result:?}");
}

#[tokio::test]
async fn authorize_mount_source_requires_read_on_the_source() {
    for_each_backend(async |test_case| {
        let registry = test_case.registry();
        let source = &Namespace::new("test-repo/source").unwrap();
        let content = b"mount authorization blob";

        let digest = put_blob_direct(&test_case.blob_store(), content).await;
        test_case
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
        let authorizer = Authorizer::new(&config).unwrap();

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
