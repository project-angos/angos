//! WWW-Authenticate header parsing, bearer-token negotiation, and auth-token cache write.

use std::{collections::HashMap, sync::LazyLock};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use regex::Regex;
use reqwest::{
    Response,
    header::{AUTHORIZATION, WWW_AUTHENTICATE},
};

use super::{RegistryClient, bearer_token::BearerToken, parse_header};
use crate::registry::Error;

fn authority_for_cache_key(url: &url::Url) -> Result<&str, Error> {
    url.host_str()
        .ok_or_else(|| Error::Internal("Response URL is missing host authority".to_string()))
}

static BEARER_PARAM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(\w+)="([^"]+)""#).unwrap());

impl RegistryClient {
    pub async fn authenticate(&self, response: &Response) -> Result<String, Error> {
        let auth_header: String = parse_header(response, WWW_AUTHENTICATE)
            .map_err(|_| Error::Unauthorized("Missing WWW-Authenticate".to_string()))?;

        if let Some(bearer_params) = auth_header.strip_prefix("Bearer ") {
            let mut params = HashMap::new();

            for cap in BEARER_PARAM_RE.captures_iter(bearer_params) {
                params.insert(cap[1].to_string(), cap[2].to_string());
            }

            let realm = params.remove("realm").ok_or_else(|| {
                Error::Internal("Missing realm parameter in WWW-Authenticate header".to_string())
            })?;

            let query = params
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");

            let mut req = self.client.get(format!("{realm}?{query}"));

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

            let authority = authority_for_cache_key(response.url())?;
            let cache_key = format!("auth:{authority}");
            let _ = self
                .cache
                .store_value(&cache_key, &token, bearer.ttl())
                .await;

            Ok(token)
        } else if auth_header.starts_with("Basic ") {
            let (user, pass) = self.basic_auth.as_ref().ok_or_else(|| {
                Error::Unauthorized("Basic auth required but not configured".to_string())
            })?;
            let encoded = BASE64_STANDARD.encode(format!("{user}:{pass}"));
            Ok(format!("Basic {encoded}"))
        } else {
            Err(Error::Internal(
                "Unsupported authentication scheme in WWW-Authenticate header".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
