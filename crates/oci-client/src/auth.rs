//! WWW-Authenticate header parsing, bearer-token negotiation, and auth-token cache keys.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use reqwest::{Response, StatusCode};
use serde::Deserialize;
use url::Url;

use crate::{Error, RegistryClient, parse_header};

fn authority_for_cache_key(url: &Url) -> Result<&str, Error> {
    url.host_str()
        .ok_or_else(|| Error::Internal("Response URL is missing host authority".to_string()))
}

/// TTL for a URL index entry written on a scope-cache hit, where the token's
/// own remaining lifetime is not observable. Overshooting is harmless: a
/// dangling entry costs one cache read before the normal challenge path.
const CACHED_TOKEN_INDEX_TTL_SECS: u64 = 3600;

fn credential_component(username: Option<&str>) -> String {
    match username {
        Some(name) => format!("user={name}"),
        None => "anon".to_string(),
    }
}

pub fn token_cache_key(
    url: &Url,
    realm: &str,
    service: Option<&str>,
    scope: Option<&str>,
    username: Option<&str>,
) -> Result<String, Error> {
    let authority = authority_for_cache_key(url)?;
    let service = service.unwrap_or_default();
    let scope = scope.unwrap_or_default();
    let credential = credential_component(username);
    Ok(format!(
        "auth:{authority}:realm={realm}:service={service}:scope={scope}:{credential}"
    ))
}

pub fn token_index_cache_key(url: &Url, username: Option<&str>) -> Result<String, Error> {
    let authority = authority_for_cache_key(url)?;
    let credential = credential_component(username);
    Ok(format!(
        "auth-index:{authority}:{credential}:{}",
        url.as_str()
    ))
}

#[derive(Deserialize)]
struct BearerToken {
    token: Option<String>,
    access_token: Option<String>,
    #[serde(default = "BearerToken::default_expires_in")]
    expires_in: u64,
}

impl BearerToken {
    fn default_expires_in() -> u64 {
        60
    }

    fn token(self) -> Result<String, Error> {
        self.token
            .or(self.access_token)
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

    fn token_url(&self) -> Result<Url, Error> {
        let mut url = Url::parse(&self.realm)
            .map_err(|e| Error::Internal(format!("Invalid bearer token realm: {e}")))?;
        url.query_pairs_mut()
            .extend_pairs(self.other.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        Ok(url)
    }
}

/// The `key="value"` parameters of a bearer challenge. A value may itself
/// hold commas (`scope="repository:x:pull,push"`), so the scan follows the
/// quotes instead of splitting on them.
fn challenge_params(mut params: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    while let Some((key, rest)) = params.split_once("=\"") {
        let Some((value, rest)) = rest.split_once('"') else {
            break;
        };
        let key = key.trim_start_matches([',', ' ']);
        if !key.is_empty() && !value.is_empty() {
            pairs.push((key.to_string(), value.to_string()));
        }
        params = rest;
    }
    pairs
}

/// Whether a token endpoint may receive this registry's credentials: an https
/// registry must not be downgraded onto an http endpoint, but a cross-host
/// https realm is fine.
fn realm_scheme_ok(registry_url: &str, token_url: &Url) -> bool {
    let registry_https = Url::parse(registry_url).is_ok_and(|url| url.scheme() == "https");
    !registry_https || token_url.scheme() == "https"
}

fn parse_bearer_challenge(header: &str) -> Option<BearerChallenge> {
    let mut realm: Option<String> = None;
    let mut other: Vec<(String, String)> = Vec::new();
    for (key, value) in challenge_params(header.strip_prefix("Bearer ")?) {
        if key == "realm" {
            realm = Some(value);
        } else {
            other.push((key, value));
        }
    }
    Some(BearerChallenge {
        realm: realm?,
        other,
    })
}

impl RegistryClient {
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
            let cache_key = token_cache_key(
                response.url(),
                &challenge.realm,
                challenge.param("service"),
                challenge.param("scope"),
                self.auth_username(),
            )?;
            if let Some(auth_header) = self.cached_auth_header_for_key(&cache_key).await
                && Some(auth_header.as_str()) != attempted_auth
            {
                // Only the exchange indexes the URL, so without this every
                // other URL sharing the scope eats a 401 on every request.
                self.index_token_url(response.url(), &cache_key, CACHED_TOKEN_INDEX_TTL_SECS)
                    .await;
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
        let token_url = challenge.token_url()?;
        if !realm_scheme_ok(&self.url, &token_url) {
            return Err(Error::Denied(format!(
                "refusing to send credentials to non-https token endpoint '{token_url}' for an https registry"
            )));
        }
        let mut req = self.client.get(token_url);
        if self.basic_auth.is_some() {
            req = req.header(AUTHORIZATION, self.build_basic_auth_header()?);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Token request failed: {e}")))?;

        // A token service that is down or throttling has refused nothing, so
        // it must not read as a credential the registry turned away.
        let status = resp.status();
        if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::Internal(format!(
                "Token acquisition failed: {status}"
            )));
        }
        if !status.is_success() {
            return Err(Error::Unauthorized(format!(
                "Token acquisition failed: {status}"
            )));
        }

        let bearer: BearerToken = resp
            .json()
            .await
            .map_err(|e| Error::Internal(format!("Failed to parse token response: {e}")))?;

        let ttl = bearer.expires_in;
        let token = format!("Bearer {}", bearer.token()?);

        let _ = self.cache.store_value(cache_key, &token, ttl).await;
        self.index_token_url(response_url, cache_key, ttl).await;

        Ok(token)
    }

    /// Point `url` at the token `cache_key` so the next request to it sends the
    /// token instead of discovering it through a 401. Best effort: an unusable
    /// index must not fail a token the caller already holds.
    async fn index_token_url(&self, url: &Url, cache_key: &str, ttl: u64) {
        let Ok(index_key) = token_index_cache_key(url, self.auth_username()) else {
            return;
        };
        let _ = self.cache.store_value(&index_key, cache_key, ttl).await;
    }

    fn build_basic_auth_header(&self) -> Result<String, Error> {
        let auth = self.basic_auth.as_ref().ok_or_else(|| {
            Error::Unauthorized("Basic auth required but not configured".to_string())
        })?;
        let encoded =
            BASE64_STANDARD.encode(format!("{}:{}", auth.username, auth.password.expose()));
        Ok(format!("Basic {encoded}"))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::from_str;
    use url::Url;

    use crate::Error;
    use crate::auth::{
        BearerToken, authority_for_cache_key, parse_bearer_challenge, realm_scheme_ok,
        token_cache_key, token_index_cache_key,
    };

    /// An omitted `expires_in` takes the spec's 60 s: caching longer keeps a
    /// token past the lifetime its issuer granted.
    #[test]
    fn bearer_token_responses_parse() {
        let parse = |json| from_str::<BearerToken>(json).unwrap();

        let token_only = parse(r#"{"token":"t"}"#);
        assert_eq!(token_only.expires_in, 60);
        assert_eq!(token_only.token().unwrap(), "t");
        assert_eq!(parse(r#"{"access_token":"a"}"#).token().unwrap(), "a");
        assert_eq!(
            parse(r#"{"token":"t","access_token":"a"}"#)
                .token()
                .unwrap(),
            "t"
        );
        assert!(matches!(parse("{}").token(), Err(Error::Internal(_))));
    }

    /// Closes the loop on angos challenging angos: the server's own challenge
    /// format must satisfy this parser, which needs the literal `Bearer ` prefix,
    /// quoted values and an absolute realm.
    #[test]
    fn the_servers_own_challenge_parses_back() {
        // The exact WWW-Authenticate the server's `build_challenge` emits.
        let challenge =
            r#"Bearer realm="https://registry.example.com/token",service="registry.example.com""#;

        let parsed = parse_bearer_challenge(challenge).unwrap();

        assert_eq!(
            parsed.token_url().unwrap().as_str(),
            "https://registry.example.com/token?service=registry.example.com"
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
                Some("repository:foo:pull"),
                None
            )
            .unwrap(),
            "auth:registry.example.com:realm=https://auth.example.com/token:service=registry:scope=repository:foo:pull:anon"
        );
    }

    /// Every client shares one cache, so a token minted for one identity must
    /// never be served to another against the same registry and scope.
    #[test]
    fn token_cache_keys_separate_identities() {
        let url = Url::parse("https://registry.example.com/v2/").unwrap();
        let key_for = |username| {
            token_cache_key(
                &url,
                "https://auth.example.com/token",
                Some("registry"),
                Some("repository:foo:pull"),
                username,
            )
            .unwrap()
        };

        let alice = key_for(Some("alice"));
        let bob = key_for(Some("bob"));
        let anonymous = key_for(None);

        assert_ne!(alice, bob, "two users must not share a cached token");
        assert_ne!(
            alice, anonymous,
            "an authenticated client must not read the anonymous token"
        );
        // The index maps a URL onto the key above, so it needs the same split.
        assert_ne!(
            token_index_cache_key(&url, Some("alice")).unwrap(),
            token_index_cache_key(&url, Some("bob")).unwrap()
        );
    }

    /// An https registry's credentials go only to an https token endpoint, but
    /// a cross-host https realm (Docker Hub's) stays allowed.
    #[test]
    fn realm_scheme_ok_refuses_only_a_downgrade() {
        for (registry, realm, ok) in [
            (
                "https://registry.example.com",
                "http://auth.example.com/token",
                false,
            ),
            (
                "https://registry-1.docker.io",
                "https://auth.docker.io/token",
                true,
            ),
            (
                "http://registry.example.com",
                "http://auth.example.com/token",
                true,
            ),
        ] {
            let token = Url::parse(realm).unwrap();
            assert_eq!(
                realm_scheme_ok(registry, &token),
                ok,
                "{registry} -> {realm}"
            );
        }
    }

    #[test]
    fn parse_bearer_challenge_needs_the_bearer_scheme_and_a_realm() {
        for header in [
            r#"Basic realm="x""#,
            "garbage",
            "",
            r#"Bearer service="registry.docker.io",scope="repository:foo:pull""#,
        ] {
            assert!(parse_bearer_challenge(header).is_none(), "{header}");
        }
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
    }
}
