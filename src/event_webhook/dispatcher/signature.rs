use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

pub fn compute_signature(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}
