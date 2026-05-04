//! Dynamically generated OIDC / JWT signing key fixtures for unit tests.
//!
//! Keys are ECDSA P-256 (ES256). Generated once per test process and cached
//! via `LazyLock`.

use std::sync::LazyLock;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::{
    PublicKey,
    elliptic_curve::{pkcs8::DecodePublicKey, sec1::ToEncodedPoint},
};
use rcgen::KeyPair;

struct OidcKey {
    pem: String,
    jwk_x: String,
    jwk_y: String,
}

fn generate_key() -> OidcKey {
    let kp = KeyPair::generate().expect("rcgen ECDSA key generation");
    let pem = kp.serialize_pem();

    // `public_key_der()` returns the SPKI (SubjectPublicKeyInfo) DER encoding.
    // Parse it with p256 to obtain the uncompressed EC point, then split into
    // the 32-byte X and Y coordinates for the JWK.
    let spki = kp.public_key_der();
    let public_key = PublicKey::from_public_key_der(&spki).expect("valid P-256 SPKI");
    let point = public_key.to_encoded_point(false); // uncompressed: 0x04 || X || Y
    let x = point.x().expect("uncompressed point has x coordinate");
    let y = point.y().expect("uncompressed point has y coordinate");

    OidcKey {
        pem,
        jwk_x: URL_SAFE_NO_PAD.encode(x),
        jwk_y: URL_SAFE_NO_PAD.encode(y),
    }
}

static PRIMARY: LazyLock<OidcKey> = LazyLock::new(generate_key);
static ALT: LazyLock<OidcKey> = LazyLock::new(generate_key);

pub fn private_key_pem() -> &'static str {
    &PRIMARY.pem
}

pub fn alt_private_key_pem() -> &'static str {
    &ALT.pem
}

pub fn jwk_x() -> &'static str {
    &PRIMARY.jwk_x
}

pub fn jwk_y() -> &'static str {
    &PRIMARY.jwk_y
}

pub const KID: &str = "unit-test-key-1";
