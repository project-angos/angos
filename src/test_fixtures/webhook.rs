//! Dynamically generated webhook mTLS certificate fixtures for unit tests.
//!
//! `ca_bundle_pem()` is a PEM chain containing the CA-signed server leaf
//! certificate followed by the CA certificate (matching the previous static
//! layout so consumers do not break).
//!
//! `client_cert_pem()` / `client_key_pem()` are a self-signed ECDSA P-256
//! client identity (CN=philippe, O=admins).

use std::sync::LazyLock;

use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair};

struct WebhookFixtures {
    ca_bundle: String,
    client_cert: String,
    client_key: String,
}

static FIXTURES: LazyLock<WebhookFixtures> = LazyLock::new(|| {
    // Server CA (self-signed)
    let ca_kp = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(vec![]).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Server CA");
    ca_params.distinguished_name = ca_dn;
    let ca_cert = ca_params.self_signed(&ca_kp).expect("ca self-sign");

    // Server leaf (CN=example.com, SAN=example.com), signed by the CA
    let leaf_kp = KeyPair::generate().expect("leaf key");
    let mut leaf_params =
        CertificateParams::new(vec!["example.com".to_string()]).expect("leaf params");
    let mut leaf_dn = DistinguishedName::new();
    leaf_dn.push(DnType::CommonName, "example.com");
    leaf_params.distinguished_name = leaf_dn;
    let leaf_cert = leaf_params
        .signed_by(&leaf_kp, &ca_cert, &ca_kp)
        .expect("ca-signed leaf");

    // Bundle = leaf || CA, matching the previous static layout.
    let ca_bundle_pem = format!("{}{}", leaf_cert.pem(), ca_cert.pem());

    // Client identity (CN=philippe, O=admins) — self-signed.
    let client_kp = KeyPair::generate().expect("client key");
    let mut client_params = CertificateParams::default();
    let mut client_dn = DistinguishedName::new();
    client_dn.push(DnType::CommonName, "philippe");
    client_dn.push(DnType::OrganizationName, "admins");
    client_params.distinguished_name = client_dn;
    let client_cert = client_params
        .self_signed(&client_kp)
        .expect("client self-sign");

    WebhookFixtures {
        ca_bundle: ca_bundle_pem,
        client_cert: client_cert.pem(),
        client_key: client_kp.serialize_pem(),
    }
});

pub fn ca_bundle_pem() -> &'static str {
    &FIXTURES.ca_bundle
}

pub fn client_cert_pem() -> &'static str {
    &FIXTURES.client_cert
}

pub fn client_key_pem() -> &'static str {
    &FIXTURES.client_key
}
