use std::collections::HashMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::identity::AuthMethod;

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
    /// Set once by the authenticator, so the trace span and the denial audit
    /// log report the same answer. Excluded from the serialized form: CEL
    /// policies see the credential fields, not the label.
    #[serde(skip)]
    pub auth_method: AuthMethod,
    /// Set when a registry token supplied this identity. `auth_method` cannot
    /// answer that on its own: a stronger method outranks the token in the
    /// label while the token still filled the identity fields.
    #[serde(skip)]
    pub from_registry_token: bool,
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
///
/// `Deserialize` exists so the token service can restore these claims from a
/// registry-issued token. It is deliberately absent from `ClientIdentity` and
/// `ClientCertificate`: certificate and client IP must come from the live
/// request, never from a token.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OidcClaims {
    pub provider_name: String,
    pub claims: HashMap<String, Value>,
}
