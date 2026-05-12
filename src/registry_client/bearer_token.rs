use serde::Deserialize;

use crate::registry::Error;

#[derive(Clone, Debug, Deserialize)]
pub struct BearerToken {
    token: Option<String>,
    access_token: Option<String>,
    #[serde(default = "BearerToken::default_expires_in")]
    expires_in: u64,
}

impl BearerToken {
    fn default_expires_in() -> u64 {
        3600
    }

    pub fn token(&self) -> Result<String, Error> {
        self.token
            .clone()
            .or(self.access_token.clone())
            .ok_or_else(|| Error::Internal("Missing token in authentication response".to_string()))
    }

    pub fn ttl(&self) -> u64 {
        self.expires_in
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
