use std::collections::HashMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::identity::AuthMethod;

/// Who sent a request, as CEL policies see it.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClientIdentity {
    pub id: Option<String>,
    pub username: Option<String>,
    pub certificate: ClientCertificate,
    pub oidc: Option<OidcClaims>,
    pub client_ip: Option<String>,
    /// Hidden from CEL: policies see the credential fields, not the label.
    #[serde(skip)]
    pub auth_method: AuthMethod,
    /// Set when a registry token supplied this identity. `auth_method` cannot
    /// answer that on its own: a stronger method outranks the token in the
    /// label while the token still filled the identity fields.
    #[serde(skip)]
    pub from_registry_token: bool,
}

impl ClientIdentity {
    pub fn new(remote_address: Option<SocketAddr>) -> Self {
        Self {
            client_ip: remote_address.map(|addr| addr.ip().to_string()),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ClientCertificate {
    pub organizations: Vec<String>,
    pub common_names: Vec<String>,
}

/// Every claim of the OIDC token, exposed to CEL as-is.
///
/// Only this type is `Deserialize`, so a registry token can restore claims but
/// never the certificate or client IP, which must come from the live request.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OidcClaims {
    pub provider_name: String,
    pub claims: HashMap<String, Value>,
}
