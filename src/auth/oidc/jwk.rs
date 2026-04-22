use jsonwebtoken::DecodingKey;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::command::server::Error;

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

    pub fn to_decoding_key(&self) -> Result<DecodingKey, Error> {
        match self {
            Jwk::Rsa { n, e, alg, kid, .. } => {
                debug!("Creating RSA DecodingKey from JWK with alg={alg:?}, kid={kid:?}");
                DecodingKey::from_rsa_components(n, e)
                    .map_err(|e| Error::Initialization(format!("Failed to create RSA key: {e}")))
            }
            Jwk::Ec { x, y, alg, kid, .. } => {
                debug!("Creating EC DecodingKey from JWK with alg={alg:?}, kid={kid:?}");
                DecodingKey::from_ec_components(x, y)
                    .map_err(|e| Error::Initialization(format!("Failed to create EC key: {e}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwk_to_decoding_key_rsa() {
        let jwk = Jwk::Rsa {
            key_use: Some("sig".to_string()),
            kid: Some("cc413527-173f-5a05-976e-9c52b1d7b431".to_string()),
            alg: Some("RS256".to_string()),
            n: "w4M936N3ZxNaEblcUoBm-xu0-V9JxNx5S7TmF0M3SBK-2bmDyAeDdeIOTcIVZHG-ZX9N9W0u1yWafgWewHrsz66BkxXq3bscvQUTAw7W3s6TEeYY7o9shPkFfOiU3x_KYgOo06SpiFdymwJflRs9cnbaU88i5fZJmUepUHVllP2tpPWTi-7UA3AdP3cdcCs5bnFfTRKzH2W0xqKsY_jIG95aQJRBDpbiesefjuyxcQnOv88j9tCKWzHpJzRKYjAUM6OPgN4HYnaSWrPJj1v41eEkFM1kORuj-GSH2qMVD02VklcqaerhQHIqM-RjeHsN7G05YtwYzomE5G-fZuwgvQ".to_string(),
            e: "AQAB".to_string(),
        };

        let result = jwk.to_decoding_key();
        assert!(result.is_ok());
    }

    #[test]
    fn test_jwk_to_decoding_key_ec() {
        let jwk = Jwk::Ec {
            key_use: Some("sig".to_string()),
            kid: Some("test-kid".to_string()),
            alg: Some("ES256".to_string()),
            x: "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4".to_string(),
            y: "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM".to_string(),
        };

        let result = jwk.to_decoding_key();
        assert!(result.is_ok());
    }

    #[test]
    fn test_jwk_deserialization() {
        let rsa_json = r#"{
            "kty": "RSA",
            "use": "sig",
            "kid": "test-rsa",
            "alg": "RS256",
            "n": "xGOr-H7A-PWG3v0lMA",
            "e": "AQAB"
        }"#;
        let jwk: Result<Jwk, _> = serde_json::from_str(rsa_json);
        assert!(jwk.is_ok());
        assert!(matches!(jwk.unwrap(), Jwk::Rsa { .. }));

        let ec_json = r#"{
            "kty": "EC",
            "use": "sig",
            "kid": "test-ec",
            "alg": "ES256",
            "x": "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4",
            "y": "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM"
        }"#;
        let jwk: Result<Jwk, _> = serde_json::from_str(ec_json);
        assert!(jwk.is_ok());
        assert!(matches!(jwk.unwrap(), Jwk::Ec { .. }));

        let unsupported_json = r#"{
            "kty": "OKP",
            "use": "sig",
            "kid": "test-okp",
            "alg": "EdDSA"
        }"#;
        let jwk: Result<Jwk, _> = serde_json::from_str(unsupported_json);
        assert!(jwk.is_err());
    }
}
