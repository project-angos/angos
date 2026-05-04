//! Dynamically generated mTLS certificate fixtures for unit tests.

use std::sync::LazyLock;

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

static CERT_DER: LazyLock<Vec<u8>> = LazyLock::new(|| {
    let mut params = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "test-user");
    dn.push(DnType::OrganizationName, "TestOrg");
    params.distinguished_name = dn;
    let key_pair = KeyPair::generate().expect("rcgen ECDSA key generation");
    let cert = params.self_signed(&key_pair).expect("rcgen self-sign");
    cert.der().to_vec()
});

static MINIMAL_CERT_DER: LazyLock<Vec<u8>> = LazyLock::new(|| {
    let mut params = CertificateParams::default();
    // Replace the default DN (which contains a CN) with an empty one so that
    // the test can verify that an empty-subject cert produces empty identity fields.
    params.distinguished_name = DistinguishedName::new();
    let key_pair = KeyPair::generate().expect("rcgen ECDSA key generation");
    let cert = params.self_signed(&key_pair).expect("rcgen self-sign");
    cert.der().to_vec()
});

pub fn cert_der() -> Vec<u8> {
    CERT_DER.clone()
}

pub fn minimal_cert_der() -> Vec<u8> {
    MINIMAL_CERT_DER.clone()
}
