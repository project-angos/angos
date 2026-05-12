//! WWW-Authenticate header parsing, bearer-token negotiation, and auth-token cache keys.

use std::sync::LazyLock;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use regex::Regex;
use reqwest::{
    Response,
    header::{AUTHORIZATION, WWW_AUTHENTICATE},
};
use serde::Deserialize;

use crate::{
    registry::Error,
    registry_client::{RegistryClient, parse_header},
};

fn authority_for_cache_key(url: &url::Url) -> Result<&str, Error> {
    url.host_str()
        .ok_or_else(|| Error::Internal("Response URL is missing host authority".to_string()))
}

pub fn token_cache_key(url: &url::Url) -> Result<String, Error> {
    let authority = authority_for_cache_key(url)?;
    Ok(format!("auth:{authority}"))
}

static BEARER_PARAM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(\w+)="([^"]+)""#).unwrap());

#[derive(Clone, Debug, Deserialize)]
struct BearerToken {
    token: Option<String>,
    access_token: Option<String>,
    #[serde(default = "BearerToken::default_expires_in")]
    expires_in: u64,
}

impl BearerToken {
    fn default_expires_in() -> u64 {
        3600
    }

    fn token(&self) -> Result<String, Error> {
        self.token
            .clone()
            .or(self.access_token.clone())
            .ok_or_else(|| Error::Internal("Missing token in authentication response".to_string()))
    }

    fn ttl(&self) -> u64 {
        self.expires_in
    }
}

struct BearerChallenge {
    realm: String,
    /// Non-realm parameters forwarded as query string to the token endpoint.
    other: Vec<(String, String)>,
}

fn parse_bearer_challenge(header: &str) -> Option<BearerChallenge> {
    let bearer_params = header.strip_prefix("Bearer ")?;
    let mut realm: Option<String> = None;
    let mut other: Vec<(String, String)> = Vec::new();
    for cap in BEARER_PARAM_RE.captures_iter(bearer_params) {
        let k = cap[1].to_string();
        let v = cap[2].to_string();
        if k == "realm" {
            realm = Some(v);
        } else {
            other.push((k, v));
        }
    }
    Some(BearerChallenge {
        realm: realm?,
        other,
    })
}

impl RegistryClient {
    pub async fn authenticate(&self, response: &Response) -> Result<String, Error> {
        let auth_header: String = parse_header(response, WWW_AUTHENTICATE)
            .map_err(|_| Error::Unauthorized("Missing WWW-Authenticate".to_string()))?;

        if let Some(challenge) = parse_bearer_challenge(&auth_header) {
            self.exchange_bearer_token(challenge, response.url()).await
        } else if auth_header.starts_with("Basic ") {
            self.build_basic_auth_header()
        } else {
            Err(Error::Internal(
                "Unsupported authentication scheme in WWW-Authenticate header".to_string(),
            ))
        }
    }

    async fn exchange_bearer_token(
        &self,
        challenge: BearerChallenge,
        response_url: &url::Url,
    ) -> Result<String, Error> {
        let query = challenge
            .other
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let mut req = self.client.get(format!("{}?{}", challenge.realm, query));
        if let Some((user, pass)) = &self.basic_auth {
            let encoded = BASE64_STANDARD.encode(format!("{user}:{pass}"));
            req = req.header(AUTHORIZATION, format!("Basic {encoded}"));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Token request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(Error::Unauthorized(format!(
                "Token acquisition failed: {}",
                resp.status()
            )));
        }

        let bearer: BearerToken = resp
            .json()
            .await
            .map_err(|e| Error::Internal(format!("Failed to parse token response: {e}")))?;

        let token = format!("Bearer {}", bearer.token()?);

        let cache_key = token_cache_key(response_url)?;
        let _ = self
            .cache
            .store_value(&cache_key, &token, bearer.ttl())
            .await;

        Ok(token)
    }

    fn build_basic_auth_header(&self) -> Result<String, Error> {
        let (user, pass) = self.basic_auth.as_ref().ok_or_else(|| {
            Error::Unauthorized("Basic auth required but not configured".to_string())
        })?;
        let encoded = BASE64_STANDARD.encode(format!("{user}:{pass}"));
        Ok(format!("Basic {encoded}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        registry::Error,
        registry_client::auth::{
            BearerToken, authority_for_cache_key, parse_bearer_challenge, token_cache_key,
        },
    };

    #[test]
    fn test_token_from_token_field() {
        let bearer = BearerToken {
            token: Some("token123".to_string()),
            access_token: None,
            expires_in: 3600,
        };

        assert_eq!(bearer.token().unwrap(), "token123");
    }

    #[test]
    fn test_token_from_access_token_field() {
        let bearer = BearerToken {
            token: None,
            access_token: Some("access456".to_string()),
            expires_in: 3600,
        };

        assert_eq!(bearer.token().unwrap(), "access456");
    }

    #[test]
    fn test_token_prefers_token_over_access_token() {
        let bearer = BearerToken {
            token: Some("token123".to_string()),
            access_token: Some("access456".to_string()),
            expires_in: 3600,
        };

        assert_eq!(bearer.token().unwrap(), "token123");
    }

    #[test]
    fn test_token_missing_both_fields() {
        let bearer = BearerToken {
            token: None,
            access_token: None,
            expires_in: 3600,
        };

        let result = bearer.token();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Internal(_)));
    }

    #[test]
    fn test_ttl_returns_expires_in() {
        let bearer = BearerToken {
            token: Some("token".to_string()),
            access_token: None,
            expires_in: 7200,
        };

        assert_eq!(bearer.ttl(), 7200);
    }

    #[test]
    fn authority_for_cache_key_returns_host() {
        let url = url::Url::parse("https://registry.example.com/v2/").unwrap();
        assert_eq!(
            authority_for_cache_key(&url).unwrap(),
            "registry.example.com"
        );
    }

    #[test]
    fn authority_for_cache_key_errors_when_host_missing() {
        let url = url::Url::parse("data:text/plain,hello").unwrap();
        let err = authority_for_cache_key(&url).expect_err("expected Err for hostless URL");
        assert!(matches!(err, Error::Internal(_)));
    }

    #[test]
    fn token_cache_key_uses_authority_prefix() {
        let url = url::Url::parse("https://registry.example.com/v2/").unwrap();
        assert_eq!(token_cache_key(&url).unwrap(), "auth:registry.example.com");
    }

    #[test]
    fn parse_bearer_challenge_returns_none_for_non_bearer_scheme() {
        assert!(parse_bearer_challenge(r#"Basic realm="x""#).is_none());
        assert!(parse_bearer_challenge("garbage").is_none());
        assert!(parse_bearer_challenge("").is_none());
    }

    #[test]
    fn parse_bearer_challenge_returns_none_when_realm_missing() {
        assert!(
            parse_bearer_challenge(
                r#"Bearer service="registry.docker.io",scope="repository:foo:pull""#
            )
            .is_none()
        );
    }

    #[test]
    fn parse_bearer_challenge_extracts_realm_and_other_params() {
        let header = r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:foo:pull""#;
        let challenge = parse_bearer_challenge(header).expect("expected Some");
        assert_eq!(challenge.realm, "https://auth.docker.io/token");
        assert!(
            challenge
                .other
                .iter()
                .any(|(k, v)| k == "service" && v == "registry.docker.io")
        );
        assert!(
            challenge
                .other
                .iter()
                .any(|(k, v)| k == "scope" && v == "repository:foo:pull")
        );
    }
}
