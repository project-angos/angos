use std::{collections::HashMap, net::SocketAddr};

use serde::Serialize;

/// Client identity information used in access control decisions.
///
/// Contains authentication details extracted from basic auth, mTLS certificates, or OIDC tokens.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClientIdentity {
    pub id: Option<String>,
    pub username: Option<String>,
    pub certificate: ClientCertificate,
    pub oidc: Option<OidcClaims>,
    pub client_ip: Option<String>,
}

impl ClientIdentity {
    /// Create a new `ClientIdentity` with the client's IP address if available
    pub fn new(remote_address: Option<SocketAddr>) -> Self {
        Self {
            client_ip: remote_address.map(|addr| addr.ip().to_string()),
            ..Default::default()
        }
    }
}

/// Certificate information extracted from client mTLS certificates.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClientCertificate {
    pub organizations: Vec<String>,
    pub common_names: Vec<String>,
}

/// OIDC claims extracted from JWT tokens.
///
/// All claims from the token are exposed as-is to allow maximum flexibility
/// in policy expressions. Standard claims like sub, iss, aud are available
/// along with any custom claims from the OIDC provider.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OidcClaims {
    pub provider_name: String,
    pub provider_type: String,
    pub claims: HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use serde_json::json;

    use super::*;

    fn assert_non_ip_fields_default(identity: &ClientIdentity) {
        assert!(identity.id.is_none());
        assert!(identity.username.is_none());
        assert!(identity.oidc.is_none());
        assert!(identity.certificate.organizations.is_empty());
        assert!(identity.certificate.common_names.is_empty());
    }

    #[test]
    fn test_new_without_remote_address() {
        let identity = ClientIdentity::new(None);

        assert!(identity.client_ip.is_none());
        assert_non_ip_fields_default(&identity);
    }

    #[test]
    fn test_new_with_ipv4_address() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let identity = ClientIdentity::new(Some(addr));

        assert_eq!(identity.client_ip.as_deref(), Some("127.0.0.1"));
        assert_non_ip_fields_default(&identity);
    }

    #[test]
    fn test_new_with_ipv6_address() {
        let addr: SocketAddr = "[::1]:443".parse().unwrap();
        let identity = ClientIdentity::new(Some(addr));

        assert_eq!(identity.client_ip.as_deref(), Some("::1"));
        assert_non_ip_fields_default(&identity);
    }

    #[test]
    fn test_full_identity_populated() {
        let identity = ClientIdentity {
            id: Some("user-123".to_string()),
            username: Some("alice".to_string()),
            certificate: ClientCertificate {
                organizations: vec!["ACME Corp".to_string()],
                common_names: vec!["alice.acme.com".to_string()],
            },
            oidc: Some(OidcClaims {
                provider_name: "github-actions".to_string(),
                provider_type: "github".to_string(),
                claims: HashMap::from([
                    (
                        "sub".to_string(),
                        json!("repo:org/repo:ref:refs/heads/main"),
                    ),
                    (
                        "iss".to_string(),
                        json!("https://token.actions.githubusercontent.com"),
                    ),
                ]),
            }),
            client_ip: Some("10.0.0.1".to_string()),
        };

        assert_eq!(identity.id.as_deref(), Some("user-123"));
        assert_eq!(identity.username.as_deref(), Some("alice"));
        assert_eq!(identity.client_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(identity.certificate.organizations, ["ACME Corp"]);
        assert_eq!(identity.certificate.common_names, ["alice.acme.com"]);

        let oidc = identity.oidc.as_ref().unwrap();
        assert_eq!(oidc.provider_name, "github-actions");
        assert_eq!(oidc.provider_type, "github");
        assert_eq!(
            oidc.claims["sub"],
            json!("repo:org/repo:ref:refs/heads/main")
        );
        assert_eq!(
            oidc.claims["iss"],
            json!("https://token.actions.githubusercontent.com")
        );
    }
}
