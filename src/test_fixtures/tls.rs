//! Dynamically generated TLS server certificate and key fixtures for unit tests.
//!
//! `server_cert_pem()` is a self-signed ECDSA P-256 certificate for
//! `CN=example.com` (with matching SAN). It doubles as its own CA in the
//! mTLS test helper, matching the previous static fixture behaviour.

use std::sync::LazyLock;

use rcgen::{CertificateParams, DistinguishedName, DnType, IsCa, KeyPair};

static SERVER: LazyLock<(String, String)> = LazyLock::new(|| {
    let mut params = CertificateParams::new(vec!["example.com".to_string()]).expect("rcgen params");
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "example.com");
    params.distinguished_name = dn;
    // Mark as CA so rustls accepts it as a root when the same cert is passed
    // to the client-CA slot in the mTLS test helper.
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key_pair = KeyPair::generate().expect("rcgen ECDSA key generation");
    let cert = params.self_signed(&key_pair).expect("rcgen self-sign");
    (cert.pem(), key_pair.serialize_pem())
});

pub fn server_cert_pem() -> &'static str {
    &SERVER.0
}

pub fn server_key_pem() -> &'static str {
    &SERVER.1
}
