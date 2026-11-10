use std::io::Write;
use std::path::PathBuf;
use std::sync::Once;

use rustls::crypto::aws_lc_rs::default_provider;
use tempfile::NamedTempFile;

use super::*;
use crate::command::server::server_context::tests::create_test_server_context;
use crate::configuration::listeners::{ClientAuth, ListenerBaseConfig};
use crate::test_fixtures::tls::{server_cert_pem, server_key_pem};

static INIT: Once = Once::new();

pub fn init_crypto_provider() {
    INIT.call_once(|| {
        default_provider().install_default().ok();
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

    (
        ServerTlsConfig {
            server_certificate_bundle: cert_file.path().to_path_buf(),
            server_private_key: key_file.path().to_path_buf(),
            client_ca_bundle: client_tls.then(|| ca_file.path().to_path_buf()),
            client_auth: ClientAuth::Optional,
        },
        (cert_file, key_file, ca_file),
    )
}

#[test]
fn build_tls_acceptor_with_and_without_client_auth() {
    init_crypto_provider();
    for (client_tls, client_auth) in [
        (false, ClientAuth::Optional),
        (true, ClientAuth::Optional),
        (true, ClientAuth::Required),
    ] {
        let (mut tls_config, _tmp_files) = build_config(client_tls);
        tls_config.client_auth = client_auth;

        let result = build_tls_acceptor(&tls_config);
        assert!(result.is_ok(), "build failed: {:?}", result.err());
    }
}

#[test]
fn build_tls_acceptor_names_the_file_it_cannot_read() {
    for file in [
        "server certificates bundle",
        "server private key",
        "client certificates bundle",
    ] {
        let (mut tls_config, _tmp_files) = build_config(false);
        let missing = PathBuf::from("/invalid/path.pem");
        match file {
            "server certificates bundle" => tls_config.server_certificate_bundle = missing,
            "server private key" => tls_config.server_private_key = missing,
            _ => tls_config.client_ca_bundle = Some(missing),
        }

        let Err(Error::Initialization(msg)) = build_tls_acceptor(&tls_config) else {
            panic!("expected an initialization error naming the {file}");
        };
        assert!(
            msg.contains(&format!("Failed to read {file}")),
            "got: {msg}"
        );
    }
}

#[test]
fn build_tls_acceptor_rejects_malformed_pem() {
    init_crypto_provider();
    let garbage = create_temp_cert_file("invalid data");
    for break_key in [false, true] {
        let (mut tls_config, _tmp_files) = build_config(true);
        if break_key {
            tls_config.server_private_key = garbage.path().to_path_buf();
        } else {
            tls_config.server_certificate_bundle = garbage.path().to_path_buf();
        }

        assert!(build_tls_acceptor(&tls_config).is_err());
    }
}

#[tokio::test]
async fn tls_listener_reloads_its_acceptor() {
    init_crypto_provider();
    let (tls, _temp_files) = build_config(false);
    let config = TlsListenerConfig {
        base: ListenerBaseConfig::default(),
        tls,
    };
    let listener = TlsListener::new(&config, create_test_server_context().await).unwrap();

    assert!(
        listener
            .notify_config_change(&config, create_test_server_context().await)
            .is_ok()
    );

    let (mut tls, _tmp_files) = build_config(false);
    assert!(listener.notify_tls_config_change(&tls).is_ok());

    tls.server_certificate_bundle = PathBuf::from("/invalid/path.pem");
    assert!(listener.notify_tls_config_change(&tls).is_err());
}

/// A `[server.tls]` without `client_auth` must keep loading: it defaults to
/// optional, a no-op when no CA bundle is configured. Only `required` needs one.
#[test]
fn client_auth_parses_and_requires_a_bundle_only_when_required() {
    for (extra, expected) in [
        ("", Some(ClientAuth::Optional)),
        (r#"client_auth = "optional""#, Some(ClientAuth::Optional)),
        (r#"client_ca_bundle = "ca.pem""#, Some(ClientAuth::Optional)),
        (
            "client_ca_bundle = \"ca.pem\"\nclient_auth = \"optional\"",
            Some(ClientAuth::Optional),
        ),
        (
            "client_ca_bundle = \"ca.pem\"\nclient_auth = \"required\"",
            Some(ClientAuth::Required),
        ),
        (r#"client_auth = "required""#, None),
    ] {
        let toml = format!(
            "server_certificate_bundle = \"server.pem\"\nserver_private_key = \"server.key\"\n{extra}"
        );
        let result: Result<ServerTlsConfig, _> = toml::from_str(&toml);
        if let Some(client_auth) = expected {
            assert_eq!(result.unwrap().client_auth, client_auth, "{extra}");
        } else {
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("client_auth = \"required\" requires client_ca_bundle"),
                "unexpected error: {msg}"
            );
        }
    }
}
