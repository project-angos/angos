use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

pub fn compute_signature(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_signature_returns_valid_hex() {
        let signature = compute_signature("my-secret", b"any-payload");
        assert!(
            signature.chars().all(|c| c.is_ascii_hexdigit()),
            "signature must be valid hex: {signature}"
        );
    }

    #[test]
    fn compute_signature_has_correct_length() {
        let signature = compute_signature("token", b"any-payload");
        assert_eq!(
            signature.len(),
            64,
            "SHA-256 HMAC hex digest must be 64 chars"
        );
    }

    #[test]
    fn compute_signature_is_deterministic() {
        let body = b"fixed payload";
        let sig1 = compute_signature("secret", body);
        let sig2 = compute_signature("secret", body);
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn compute_signature_differs_for_different_secrets() {
        let body = b"same payload";
        let sig1 = compute_signature("secret-a", body);
        let sig2 = compute_signature("secret-b", body);
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn compute_signature_differs_for_different_bodies() {
        let sig1 = compute_signature("secret", b"body-a");
        let sig2 = compute_signature("secret", b"body-b");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn compute_signature_matches_known_value() {
        // Pre-computed with: echo -n "hello world" | openssl dgst -sha256 -hmac "test-secret"
        let expected = "046e2496e13e0bfd8dbef84244dd188311a48086646355161bc4ad0769a49cf4";
        let signature = compute_signature("test-secret", b"hello world");
        assert_eq!(signature, expected);
    }

    #[test]
    fn compute_signature_empty_body() {
        let sig = compute_signature("secret", b"");
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
