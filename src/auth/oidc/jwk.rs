use jsonwebtoken::DecodingKey;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::auth::Error;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kty")]
pub enum Jwk {
    #[serde(rename = "RSA")]
    Rsa {
        #[serde(rename = "use", skip_serializing_if = "Option::is_none")]
        key_use: Option<String>,
        kid: Option<String>,
        alg: Option<String>,
        n: String,
        e: String,
    },
    #[serde(rename = "EC")]
    Ec {
        #[serde(rename = "use", skip_serializing_if = "Option::is_none")]
        key_use: Option<String>,
        kid: Option<String>,
        alg: Option<String>,
        x: String,
        y: String,
    },
}

impl Jwk {
    pub fn kid(&self) -> Option<&str> {
        match self {
            Jwk::Rsa { kid, .. } | Jwk::Ec { kid, .. } => kid.as_deref(),
        }
    }

    /// A key the provider served but angos cannot use is the provider's failure,
    /// not the caller's credential, so it reports the provider unavailable like
    /// the JWKS fetch and parse that preceded it.
    pub fn to_decoding_key(&self) -> Result<DecodingKey, Error> {
        match self {
            Jwk::Rsa { n, e, alg, kid, .. } => {
                debug!("Creating RSA DecodingKey from JWK with alg={alg:?}, kid={kid:?}");
                DecodingKey::from_rsa_components(n, e).map_err(|e| {
                    Error::ProviderUnavailable(format!("Failed to create RSA key: {e}"))
                })
            }
            Jwk::Ec { x, y, alg, kid, .. } => {
                debug!("Creating EC DecodingKey from JWK with alg={alg:?}, kid={kid:?}");
                DecodingKey::from_ec_components(x, y).map_err(|e| {
                    Error::ProviderUnavailable(format!("Failed to create EC key: {e}"))
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The key material is fetched per request, so an unusable one must not be
    /// reported as a startup failure and answered with a 500.
    #[test]
    fn an_unusable_key_reports_the_provider_unavailable() {
        let undecodable = "not base64".to_string();
        let keys = [
            Jwk::Rsa {
                key_use: None,
                kid: None,
                alg: None,
                n: undecodable.clone(),
                e: "AQAB".to_string(),
            },
            Jwk::Ec {
                key_use: None,
                kid: None,
                alg: None,
                x: undecodable.clone(),
                y: undecodable,
            },
        ];

        for jwk in keys {
            let Err(error) = jwk.to_decoding_key() else {
                panic!("a JWK angos cannot decode must not yield a key: {jwk:?}");
            };
            assert!(
                matches!(error, Error::ProviderUnavailable(_)),
                "got: {error:?}"
            );
        }
    }
}
