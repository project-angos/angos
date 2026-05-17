use std::{
    io::Write,
    net::{IpAddr, Ipv6Addr},
    num::NonZeroU64,
    path::PathBuf,
    sync::Once,
};

use tempfile::NamedTempFile;

use super::*;
use crate::{
    command::server::server_context::tests::create_test_server_context,
    configuration::listeners::{ClientAuth, ListenerBaseConfig},
    test_fixtures::tls::{server_cert_pem, server_key_pem},
};

static INIT: Once = Once::new();

fn init_crypto_provider() {
    INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();
    });
}

fn create_temp_cert_file(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

pub fn build_config(
    client_tls: bool,
) -> (
    ServerTlsConfig,
    (NamedTempFile, NamedTempFile, NamedTempFile),
) {
    let cert_file = create_temp_cert_file(server_cert_pem());
    let key_file = create_temp_cert_file(server_key_pem());
    let ca_file = create_temp_cert_file(server_cert_pem());

    let server_certificate_bundle = cert_file.path().to_path_buf();
    let server_private_key = key_file.path().to_path_buf();
    let client_ca_bundle = client_tls.then(|| ca_file.path().to_path_buf());
    let client_auth = ClientAuth::Optional;

    (
        ServerTlsConfig {
            server_certificate_bundle,
            server_private_key,
            client_ca_bundle,
            client_auth,
        },
        (cert_file, key_file, ca_file),
    )
}

#[test]
fn test_config_default_values() {
    let cert_file = create_temp_cert_file(server_cert_pem());
    let key_file = create_temp_cert_file(server_key_pem());

    let toml = format!(
        r#"
        bind_address = "0.0.0.0"
        [tls]
        server_certificate_bundle = "{}"
        server_private_key = "{}"
    "#,
        cert_file.path().display(),
        key_file.path().display()
    );

    let config: TlsListenerConfig = toml::from_str(&toml).unwrap();

    assert_eq!(config.base.port, 8000);
    assert_eq!(config.base.query_timeout_secs.get(), 3600);
    assert_eq!(config.base.query_timeout_grace_period_secs.get(), 60);
}

#[test]
fn test_config_custom_values() {
    let cert_file = create_temp_cert_file(server_cert_pem());
    let key_file = create_temp_cert_file(server_key_pem());

    let toml = format!(
        r#"
        bind_address = "192.168.1.100"
        port = 9000
        query_timeout_secs = 7200
        query_timeout_grace_period_secs = 120
        [tls]
        server_certificate_bundle = "{}"
        server_private_key = "{}"
    "#,
        cert_file.path().display(),
        key_file.path().display()
    );

    let config: TlsListenerConfig = toml::from_str(&toml).unwrap();

    assert_eq!(config.base.port, 9000);
    assert_eq!(config.base.query_timeout_secs.get(), 7200);
    assert_eq!(config.base.query_timeout_grace_period_secs.get(), 120);
    assert_eq!(
        config.base.bind_address,
        "192.168.1.100".parse::<IpAddr>().unwrap()
    );
}

#[test]
fn test_config_ipv6_address() {
    let cert_file = create_temp_cert_file(server_cert_pem());
    let key_file = create_temp_cert_file(server_key_pem());

    let toml = format!(
        r#"
        bind_address = "::1"
        port = 8443
        [tls]
        server_certificate_bundle = "{}"
        server_private_key = "{}"
    "#,
        cert_file.path().display(),
        key_file.path().display()
    );

    let config: TlsListenerConfig = toml::from_str(&toml).unwrap();

    assert_eq!(config.base.bind_address, IpAddr::from(Ipv6Addr::LOCALHOST));
    assert_eq!(config.base.port, 8443);
}

#[test]
fn test_build_tls_acceptor_success() {
    init_crypto_provider();
    let (tls_config, _tmp_files) = build_config(false);
    let result = TlsListener::build_tls_acceptor(&tls_config);

    assert!(result.is_ok());
}

#[test]
fn test_build_tls_acceptor_with_client_ca() {
    init_crypto_provider();
    let (tls_config, _tmp_files) = build_config(true);

    let result = TlsListener::build_tls_acceptor(&tls_config);

    assert!(result.is_ok());
}

#[test]
fn test_build_tls_acceptor_missing_cert_file() {
    let (mut tls_config, _tmp_files) = build_config(true);
    tls_config.server_certificate_bundle = PathBuf::from("/invalid/path.pem");

    let result = TlsListener::build_tls_acceptor(&tls_config);

    assert!(result.is_err());
    if let Err(Error::Initialization(msg)) = result {
        assert!(msg.contains("Failed to read server certificates bundle"));
    } else {
        panic!("Expected Initialization error");
    }
}

#[test]
fn test_build_tls_acceptor_missing_key_file() {
    let (mut tls_config, _tmp_files) = build_config(true);
    tls_config.server_private_key = PathBuf::from("/invalid/path.pem");

    let result = TlsListener::build_tls_acceptor(&tls_config);

    assert!(result.is_err());
    if let Err(Error::Initialization(msg)) = result {
        assert!(msg.contains("Failed to read server private key"));
    } else {
        panic!("Expected Initialization error");
    }
}

#[test]
fn test_build_tls_acceptor_invalid_cert_format() {
    init_crypto_provider();
    let cert_file = create_temp_cert_file("invalid cert data");

    let (mut tls_config, _tmp_files) = build_config(true);
    tls_config.server_certificate_bundle = cert_file.path().to_path_buf();

    let result = TlsListener::build_tls_acceptor(&tls_config);

    assert!(result.is_err());
}

#[test]
fn test_build_tls_acceptor_invalid_key_format() {
    init_crypto_provider();
    let key_file = create_temp_cert_file("invalid key data");

    let (mut tls_config, _tmp_files) = build_config(true);
    tls_config.server_private_key = key_file.path().to_path_buf();

    let result = TlsListener::build_tls_acceptor(&tls_config);

    assert!(result.is_err());
}

#[test]
fn test_build_tls_acceptor_missing_client_ca_file() {
    let (mut tls_config, _tmp_files) = build_config(false);
    tls_config.client_ca_bundle = Some(PathBuf::from("/nonexistent/ca.pem"));

    let result = TlsListener::build_tls_acceptor(&tls_config);

    assert!(result.is_err());
    if let Err(Error::Initialization(msg)) = result {
        assert!(msg.contains("Failed to read client certificates bundle"));
    } else {
        panic!("Expected Initialization error");
    }
}

#[tokio::test]
async fn test_tls_listener_new() {
    init_crypto_provider();

    let (tls, _temp_files) = build_config(false);

    let config = TlsListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout_secs: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period_secs: NonZeroU64::new(60).unwrap(),
        },
        tls,
    };

    let context = create_test_server_context().await;
    let result = TlsListener::new(&config, context);

    assert!(result.is_ok());
    let listener = result.unwrap();
    assert_eq!(
        listener.binding_address,
        SocketAddr::from(([127, 0, 0, 1], 8443))
    );
}

#[tokio::test]
async fn test_tls_listener_new_with_ipv6() {
    init_crypto_provider();
    let (tls, _temp_files) = build_config(false);

    let config = TlsListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "::1".parse().unwrap(),
            port: 9443,
            query_timeout_secs: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period_secs: NonZeroU64::new(60).unwrap(),
        },
        tls,
    };

    let context = create_test_server_context().await;
    let result = TlsListener::new(&config, context);

    assert!(result.is_ok());
    let listener = result.unwrap();
    assert_eq!(
        listener.binding_address.ip(),
        "::1".parse::<IpAddr>().unwrap()
    );
    assert_eq!(listener.binding_address.port(), 9443);
}

#[tokio::test]
async fn test_tls_listener_new_with_invalid_certs() {
    init_crypto_provider();
    let cert_file = create_temp_cert_file("invalid");
    let key_file = create_temp_cert_file("invalid");

    let (mut tls, _temp_files) = build_config(false);
    tls.server_certificate_bundle = cert_file.path().to_path_buf();
    tls.server_private_key = key_file.path().to_path_buf();

    let config = TlsListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout_secs: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period_secs: NonZeroU64::new(60).unwrap(),
        },
        tls,
    };

    let context = create_test_server_context().await;
    let result = TlsListener::new(&config, context);

    assert!(result.is_err());
}

#[tokio::test]
async fn test_tls_listener_notify_config_change() {
    init_crypto_provider();
    let (tls, _temp_files) = build_config(false);

    let config = TlsListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout_secs: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period_secs: NonZeroU64::new(60).unwrap(),
        },
        tls,
    };

    let context1 = create_test_server_context().await;
    let listener = TlsListener::new(&config, context1).unwrap();

    let context2 = create_test_server_context().await;
    let result = listener.notify_config_change(&config, context2);

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_tls_listener_notify_tls_config_change() {
    init_crypto_provider();
    let (tls, _temp_files) = build_config(false);

    let config = TlsListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout_secs: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period_secs: NonZeroU64::new(60).unwrap(),
        },
        tls,
    };

    let context = create_test_server_context().await;
    let listener = TlsListener::new(&config, context).unwrap();

    let (tls, _temp_files) = build_config(false);
    let result = listener.notify_tls_config_change(&tls);

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_tls_listener_notify_tls_config_change_with_invalid_certs() {
    init_crypto_provider();

    let (tls, _temp_files) = build_config(false);
    let config = TlsListenerConfig {
        base: ListenerBaseConfig {
            bind_address: "127.0.0.1".parse().unwrap(),
            port: 8443,
            query_timeout_secs: NonZeroU64::new(3600).unwrap(),
            query_timeout_grace_period_secs: NonZeroU64::new(60).unwrap(),
        },
        tls,
    };

    let context = create_test_server_context().await;
    let listener = TlsListener::new(&config, context).unwrap();

    let (mut tls_config, _tmp_files) = build_config(true);
    tls_config.server_certificate_bundle = PathBuf::from("/invalid/path.pem");

    let result = listener.notify_tls_config_change(&tls_config);

    assert!(result.is_err());
}

// --- client_auth validation matrix ---

#[test]
fn bare_tls_without_client_auth_field_is_ok() {
    // Backward compat: existing [server.tls] configs without client_ca_bundle
    // and without client_auth must continue to load. client_auth defaults to
    // Optional and is ignored at runtime when no CA bundle is configured.
    let toml = r#"
        server_certificate_bundle = "server.pem"
        server_private_key = "server.key"
    "#;
    let result: Result<ServerTlsConfig, _> = toml::from_str(toml);
    assert!(result.is_ok(), "unexpected error: {result:?}");
    let config = result.unwrap();
    assert!(config.client_ca_bundle.is_none());
    assert_eq!(config.client_auth, ClientAuth::Optional);
}

#[test]
fn client_auth_optional_without_ca_bundle_is_ok_field_is_ignored() {
    // Explicit "optional" without a CA bundle is harmless: there is no CA
    // bundle to validate against, so the field is a no-op at runtime.
    let toml = r#"
        server_certificate_bundle = "server.pem"
        server_private_key = "server.key"
        client_auth = "optional"
    "#;
    let result: Result<ServerTlsConfig, _> = toml::from_str(toml);
    assert!(result.is_ok(), "unexpected error: {result:?}");
}

#[test]
fn client_auth_optional_with_ca_bundle_is_ok() {
    let toml = r#"
        server_certificate_bundle = "server.pem"
        server_private_key = "server.key"
        client_ca_bundle = "ca.pem"
        client_auth = "optional"
    "#;
    let result: Result<ServerTlsConfig, _> = toml::from_str(toml);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().client_auth, ClientAuth::Optional);
}

#[test]
fn client_auth_required_without_ca_bundle_is_rejected() {
    let toml = r#"
        server_certificate_bundle = "server.pem"
        server_private_key = "server.key"
        client_auth = "required"
    "#;
    let result: Result<ServerTlsConfig, _> = toml::from_str(toml);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("client_auth = \"required\" requires client_ca_bundle"),
        "unexpected error: {msg}"
    );
}

#[test]
fn client_auth_required_with_ca_bundle_is_ok() {
    let toml = r#"
        server_certificate_bundle = "server.pem"
        server_private_key = "server.key"
        client_ca_bundle = "ca.pem"
        client_auth = "required"
    "#;
    let result: Result<ServerTlsConfig, _> = toml::from_str(toml);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().client_auth, ClientAuth::Required);
}

#[test]
fn client_auth_defaults_to_optional_when_ca_bundle_present_and_field_omitted() {
    let toml = r#"
        server_certificate_bundle = "server.pem"
        server_private_key = "server.key"
        client_ca_bundle = "ca.pem"
    "#;
    let result: Result<ServerTlsConfig, _> = toml::from_str(toml);
    assert!(result.is_ok(), "unexpected error: {result:?}");
    assert_eq!(result.unwrap().client_auth, ClientAuth::Optional);
}

// --- build_tls_acceptor with required mode ---

#[test]
fn test_build_tls_acceptor_with_client_auth_required() {
    init_crypto_provider();
    let (mut tls_config, _tmp_files) = build_config(true);
    tls_config.client_auth = ClientAuth::Required;

    let result = TlsListener::build_tls_acceptor(&tls_config);
    assert!(result.is_ok(), "build failed: {:?}", result.err());
}

#[test]
fn test_build_tls_acceptor_without_client_ca_ignores_client_auth() {
    init_crypto_provider();
    let (tls_config, _tmp_files) = build_config(false);
    assert!(tls_config.client_ca_bundle.is_none());

    let result = TlsListener::build_tls_acceptor(&tls_config);
    assert!(result.is_ok(), "build failed: {:?}", result.err());
}
