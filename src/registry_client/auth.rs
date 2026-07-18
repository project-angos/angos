//! WWW-Authenticate header parsing, bearer-token negotiation, and auth-token cache keys.

use std::sync::LazyLock;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use regex::Regex;
use reqwest::{
    Response,
    header::{AUTHORIZATION, WWW_AUTHENTICATE},
};
use serde::Deserialize;
use url::Url;

use crate::registry_client::{Error, RegistryClient, parse_header};

fn authority_for_cache_key(url: &Url) -> Result<&str, Error> {
    url.host_str()
        .ok_or_else(|| Error::Internal("Response URL is missing host authority".to_string()))
}

pub fn token_cache_key(
    url: &Url,
    realm: &str,
    service: Option<&str>,
    scope: Option<&str>,
) -> Result<String, Error> {
    let authority = authority_for_cache_key(url)?;
    let service = service.unwrap_or_default();
    let scope = scope.unwrap_or_default();
    Ok(format!(
        "auth:{authority}:realm={realm}:service={service}:scope={scope}"
    ))
}

pub fn token_index_cache_key(url: &Url) -> Result<String, Error> {
    let authority = authority_for_cache_key(url)?;
    Ok(format!("auth-index:{authority}:{}", url.as_str()))
}

/// `None` only if the literal pattern were malformed; the challenge parser
/// then degrades to "no bearer challenge" instead of panicking.
static BEARER_PARAM_RE: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r#"(\w+)="([^"]+)""#).ok());

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
}

struct BearerChallenge {
    realm: String,
    /// Non-realm parameters forwarded as query string to the token endpoint.
    other: Vec<(String, String)>,
}

impl BearerChallenge {
    fn param(&self, name: &str) -> Option<&str> {
        self.other
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
    }

    fn cache_key(&self, response_url: &Url) -> Result<String, Error> {
        token_cache_key(
            response_url,
            &self.realm,
            self.param("service"),
            self.param("scope"),
        )
    }

    fn token_url(&self) -> Result<Url, Error> {
        let mut url = Url::parse(&self.realm)
            .map_err(|e| Error::Internal(format!("Invalid bearer token realm: {e}")))?;
        url.query_pairs_mut()
            .extend_pairs(self.other.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        Ok(url)
    }
}

fn parse_bearer_challenge(header: &str) -> Option<BearerChallenge> {
    let bearer_params = header.strip_prefix("Bearer ")?;
    let mut realm: Option<String> = None;
    let mut other: Vec<(String, String)> = Vec::new();
    for cap in BEARER_PARAM_RE.as_ref()?.captures_iter(bearer_params) {
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
    /// Builds an authentication header from an upstream authentication challenge.
    ///
    /// # Errors
    ///
    /// Returns an error when the challenge is missing, unsupported, or when bearer
    /// token acquisition fails.
    pub async fn authenticate(&self, response: &Response) -> Result<String, Error> {
        self.authenticate_with_cache(response, None).await
    }

    /// Builds an authentication header, reusing cached bearer tokens when possible.
    ///
    /// # Errors
    ///
    /// Returns an error when the challenge is missing, unsupported, or when bearer
    /// token acquisition fails.
    pub async fn authenticate_with_cache(
        &self,
        response: &Response,
        attempted_auth: Option<&str>,
    ) -> Result<String, Error> {
        let auth_header: String = parse_header(response, WWW_AUTHENTICATE)
            .map_err(|_| Error::Unauthorized("Missing WWW-Authenticate".to_string()))?;

        if let Some(challenge) = parse_bearer_challenge(&auth_header) {
            let cache_key = challenge.cache_key(response.url())?;
            if let Some(auth_header) = self.cached_auth_header_for_key(&cache_key).await
                && Some(auth_header.as_str()) != attempted_auth
            {
                return Ok(auth_header);
            }
            self.exchange_bearer_token(challenge, response.url(), &cache_key)
                .await
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
        response_url: &Url,
        cache_key: &str,
    ) -> Result<String, Error> {
        let mut req = self.client.get(challenge.token_url()?);
        if self.basic_auth.is_some() {
            req = req.header(AUTHORIZATION, self.build_basic_auth_header()?);
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
        let ttl = bearer.expires_in;

        let _ = self.cache.store_value(cache_key, &token, ttl).await;
        let _ = self
            .cache
            .store_value(&token_index_cache_key(response_url)?, cache_key, ttl)
            .await;

        Ok(token)
    }

    fn build_basic_auth_header(&self) -> Result<String, Error> {
        let (user, pass) = self.basic_auth.as_ref().ok_or_else(|| {
            Error::Unauthorized("Basic auth required but not configured".to_string())
        })?;
        let encoded = BASE64_STANDARD.encode(format!("{user}:{}", pass.expose()));
        Ok(format!("Basic {encoded}"))
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use crate::registry_client::{
        Error,
        auth::{BearerToken, authority_for_cache_key, parse_bearer_challenge, token_cache_key},
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
    fn authority_for_cache_key_returns_host() {
        let url = Url::parse("https://registry.example.com/v2/").unwrap();
        assert_eq!(
            authority_for_cache_key(&url).unwrap(),
            "registry.example.com"
        );
    }

    #[test]
    fn authority_for_cache_key_errors_when_host_missing() {
        let url = Url::parse("data:text/plain,hello").unwrap();
        let err = authority_for_cache_key(&url).expect_err("expected Err for hostless URL");
        assert!(matches!(err, Error::Internal(_)));
    }

    #[test]
    fn token_cache_key_includes_bearer_scope() {
        let url = Url::parse("https://registry.example.com/v2/").unwrap();
        assert_eq!(
            token_cache_key(
                &url,
                "https://auth.example.com/token",
                Some("registry"),
                Some("repository:foo:pull")
            )
            .unwrap(),
            "auth:registry.example.com:realm=https://auth.example.com/token:service=registry:scope=repository:foo:pull"
        );
    }

    #[test]
    fn token_cache_key_separates_different_scopes() {
        let url = Url::parse("https://registry.example.com/v2/").unwrap();
        let foo = token_cache_key(
            &url,
            "https://auth.example.com/token",
            Some("registry"),
            Some("repository:foo:pull"),
        )
        .unwrap();
        let bar = token_cache_key(
            &url,
            "https://auth.example.com/token",
            Some("registry"),
            Some("repository:bar:pull"),
        )
        .unwrap();

        assert_ne!(foo, bar);
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

    #[test]
    fn bearer_challenge_token_url_preserves_simple_ascii_values() {
        let header =
            r#"Bearer realm="https://auth.example.com/token",service="registry.example.com""#;
        let challenge = parse_bearer_challenge(header).expect("expected Some");

        assert_eq!(
            challenge.token_url().unwrap().as_str(),
            "https://auth.example.com/token?service=registry.example.com"
        );
    }

    #[test]
    fn bearer_challenge_token_url_encodes_special_characters() {
        let header = r#"Bearer realm="https://auth.example.com/token",service="registry.example.com",scope="repository:team/app image:pull,push""#;
        let url = parse_bearer_challenge(header)
            .expect("expected Some")
            .token_url()
            .unwrap();

        assert_eq!(
            url.as_str(),
            "https://auth.example.com/token?service=registry.example.com&scope=repository%3Ateam%2Fapp+image%3Apull%2Cpush"
        );
        assert!(url.query().unwrap().contains("%2F"));
        assert!(url.query().unwrap().contains('+'));
    }
}
