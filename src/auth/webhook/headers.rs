use std::str::FromStr;

use hyper::{
    header::{HeaderName, HeaderValue},
    http::{HeaderMap, request::Parts},
};

use crate::{
    auth::sha256_hex,
    command::server::Error,
    identity::{Action, ClientIdentity},
};

static X_FORWARDED_METHOD: &str = "X-Forwarded-Method";
static X_FORWARDED_PROTO: &str = "X-Forwarded-Proto";
static X_FORWARDED_HOST: &str = "X-Forwarded-Host";
static X_FORWARDED_URI: &str = "X-Forwarded-Uri";
static X_FORWARDED_FOR: &str = "X-Forwarded-For";
static X_REGISTRY_ACTION: &str = "X-Registry-Action";
static X_REGISTRY_NAMESPACE: &str = "X-Registry-Namespace";
static X_REGISTRY_REFERENCE: &str = "X-Registry-Reference";
static X_REGISTRY_DIGEST: &str = "X-Registry-Digest";
static X_REGISTRY_USERNAME: &str = "X-Registry-Username";
static X_REGISTRY_IDENTITY_ID: &str = "X-Registry-Identity-ID";
static X_REGISTRY_CERTIFICATE_CN: &str = "X-Registry-Certificate-CN";
static X_REGISTRY_CERTIFICATE_O: &str = "X-Registry-Certificate-O";

pub fn build_header_name(name: &str) -> Result<HeaderName, Error> {
    match HeaderName::from_str(name) {
        Ok(h) => Ok(h),
        Err(e) => {
            let msg = format!("Invalid header name '{name}': {e}");
            Err(Error::Execution(msg))
        }
    }
}

pub fn build_header_value(value: &str) -> Result<HeaderValue, Error> {
    match HeaderValue::from_str(value) {
        Ok(hv) => Ok(hv),
        Err(e) => {
            let msg = format!("Invalid header value '{value}': {e}");
            Err(Error::Execution(msg))
        }
    }
}

pub fn set_forwarded_method_header(parts: &Parts, headers: &mut HeaderMap) -> Result<(), Error> {
    let value = build_header_value(parts.method.as_str())?;
    headers.insert(X_FORWARDED_METHOD, value);
    Ok(())
}

pub fn set_forwarded_proto_header(parts: &Parts, headers: &mut HeaderMap) -> Result<(), Error> {
    let proto = if parts.uri.scheme_str() == Some("https") {
        build_header_value("https")?
    } else {
        build_header_value("http")?
    };

    headers.insert(X_FORWARDED_PROTO, proto);
    Ok(())
}

pub fn set_forwarded_host_header(parts: &Parts, headers: &mut HeaderMap) {
    if let Some(host) = parts.headers.get("Host") {
        headers.insert(X_FORWARDED_HOST, host.clone());
    }
}

pub fn set_forwarded_uri_header(parts: &Parts, headers: &mut HeaderMap) -> Result<(), Error> {
    let uri = parts.uri.to_string();
    let value = build_header_value(&uri)?;
    headers.insert(X_FORWARDED_URI, value);
    Ok(())
}

pub fn set_forwarded_for_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(ip) = &identity.client_ip {
        let value = build_header_value(ip)?;
        headers.insert(X_FORWARDED_FOR, value);
    }
    Ok(())
}

pub fn set_registry_action_header(action: &Action, headers: &mut HeaderMap) -> Result<(), Error> {
    let value = build_header_value(action.action_name())?;
    headers.insert(X_REGISTRY_ACTION, value);
    Ok(())
}

pub fn set_registry_namespace_header(
    action: &Action,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(namespace) = action.get_namespace() {
        let value = build_header_value(namespace)?;
        headers.insert(X_REGISTRY_NAMESPACE, value);
    }
    Ok(())
}

pub fn set_registry_reference_header(
    action: &Action,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(reference) = action.get_reference() {
        let value = build_header_value(&reference.to_string())?;
        headers.insert(X_REGISTRY_REFERENCE, value);
    }
    Ok(())
}

pub fn set_registry_digest_header(action: &Action, headers: &mut HeaderMap) -> Result<(), Error> {
    if let Some(digest) = action.get_digest() {
        let value = build_header_value(&digest.to_string())?;
        headers.insert(X_REGISTRY_DIGEST, value);
    }
    Ok(())
}

pub fn set_registry_username_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(username) = &identity.username {
        let value = build_header_value(username)?;
        headers.insert(X_REGISTRY_USERNAME, value);
    }
    Ok(())
}

pub fn set_registry_identity_id_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    if let Some(id) = &identity.id {
        let value = build_header_value(id)?;
        headers.insert(X_REGISTRY_IDENTITY_ID, value);
    }
    Ok(())
}

pub fn set_registry_certificate_cn_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    for cn in &identity.certificate.common_names {
        let value = build_header_value(cn)?;
        headers.append(X_REGISTRY_CERTIFICATE_CN, value);
    }
    Ok(())
}

pub fn set_registry_certificate_o_header(
    identity: &ClientIdentity,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    for org in &identity.certificate.organizations {
        let value = build_header_value(org)?;
        headers.append(X_REGISTRY_CERTIFICATE_O, value);
    }
    Ok(())
}

pub fn set_forwarded_headers(
    forward_headers: &[String],
    parts: &Parts,
    headers: &mut HeaderMap,
) -> Result<(), Error> {
    for name in forward_headers {
        if let Some(value) = parts.headers.get(name) {
            let name = build_header_name(name)?;
            headers.insert(name, value.clone());
        }
    }
    Ok(())
}

pub fn build_headers(
    forward_headers: &[String],
    action: &Action,
    identity: &ClientIdentity,
    parts: &Parts,
) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::new();

    set_forwarded_method_header(parts, &mut headers)?;
    set_forwarded_proto_header(parts, &mut headers)?;
    set_forwarded_host_header(parts, &mut headers);
    set_forwarded_uri_header(parts, &mut headers)?;
    set_forwarded_for_header(identity, &mut headers)?;

    set_registry_action_header(action, &mut headers)?;
    set_registry_namespace_header(action, &mut headers)?;
    set_registry_reference_header(action, &mut headers)?;
    set_registry_digest_header(action, &mut headers)?;

    set_registry_username_header(identity, &mut headers)?;
    set_registry_identity_id_header(identity, &mut headers)?;

    set_registry_certificate_cn_header(identity, &mut headers)?;
    set_registry_certificate_o_header(identity, &mut headers)?;

    set_forwarded_headers(forward_headers, parts, &mut headers)?;
    Ok(headers)
}

pub fn build_cache_key(
    name: &str,
    action: &Action,
    identity: &ClientIdentity,
) -> Result<String, Error> {
    let Ok(key_material) = serde_json::to_vec(&(identity, action)) else {
        let msg = "Failed to serialize webhook cache key".to_string();
        return Err(Error::Execution(msg));
    };

    Ok(format!("webhook:{name}:{}", sha256_hex(key_material)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::build_cache_key;
    use crate::{
        identity::{Action, ClientIdentity, OidcClaims},
        oci::{Namespace, Reference},
    };

    fn anonymous() -> ClientIdentity {
        ClientIdentity::new(None)
    }

    fn identity_with_username(username: &str) -> ClientIdentity {
        let mut id = ClientIdentity::new(None);
        id.username = Some(username.to_string());
        id
    }

    #[test]
    fn same_inputs_produce_same_key() {
        let action = Action::ApiVersion;
        let identity = anonymous();

        let k1 = build_cache_key("wh", &action, &identity).unwrap();
        let k2 = build_cache_key("wh", &action, &identity).unwrap();

        assert_eq!(k1, k2);
    }

    #[test]
    fn different_action_produces_different_key() {
        let identity = anonymous();

        let k_api = build_cache_key("wh", &Action::ApiVersion, &identity).unwrap();
        let k_manifest = build_cache_key(
            "wh",
            &Action::GetManifest {
                namespace: Namespace::new("library/nginx").unwrap(),
                reference: Reference::Tag("latest".to_string()),
            },
            &identity,
        )
        .unwrap();

        assert_ne!(k_api, k_manifest);
    }

    #[test]
    fn different_identity_produces_different_key() {
        let action = Action::ApiVersion;

        let k_anon = build_cache_key("wh", &action, &anonymous()).unwrap();
        let k_user = build_cache_key("wh", &action, &identity_with_username("alice")).unwrap();

        assert_ne!(k_anon, k_user);
    }

    #[test]
    fn different_webhook_name_produces_different_key() {
        let action = Action::ApiVersion;
        let identity = anonymous();

        let k_a = build_cache_key("webhook-a", &action, &identity).unwrap();
        let k_b = build_cache_key("webhook-b", &action, &identity).unwrap();

        assert_ne!(k_a, k_b);
    }

    #[test]
    fn key_contains_webhook_prefix_and_name() {
        let action = Action::ApiVersion;
        let identity = anonymous();

        let key = build_cache_key("my-hook", &action, &identity).unwrap();

        assert!(
            key.starts_with("webhook:my-hook:"),
            "key must be prefixed with 'webhook:<name>:': {key}"
        );
    }

    #[test]
    fn key_uses_bounded_sha256_digest() {
        let action = Action::ApiVersion;
        let identity = anonymous();

        let key = build_cache_key("my-hook", &action, &identity).unwrap();
        let digest = key.strip_prefix("webhook:my-hook:").unwrap();

        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn key_does_not_expose_oidc_claims() {
        let action = Action::ApiVersion;
        let identity = ClientIdentity {
            oidc: Some(OidcClaims {
                provider_name: "github-actions".to_string(),
                provider_type: "GitHub Actions".to_string(),
                claims: HashMap::from([
                    ("sub".to_string(), json!("repo:private/repo:ref:main")),
                    ("email".to_string(), json!("person@example.com")),
                    ("custom_claim".to_string(), json!("internal-secret")),
                ]),
            }),
            ..ClientIdentity::new(None)
        };

        let key = build_cache_key("my-hook", &action, &identity).unwrap();

        assert!(!key.contains("person@example.com"), "key was: {key}");
        assert!(!key.contains("repo:private/repo"), "key was: {key}");
        assert!(!key.contains("internal-secret"), "key was: {key}");
        assert!(!key.contains("custom_claim"), "key was: {key}");
        assert!(!key.contains("email"), "key was: {key}");
        assert!(!key.contains("sub"), "key was: {key}");
    }

    #[test]
    fn different_reference_same_namespace_produces_different_key() {
        let identity = anonymous();

        let k_latest = build_cache_key(
            "wh",
            &Action::GetManifest {
                namespace: Namespace::new("library/nginx").unwrap(),
                reference: Reference::Tag("latest".to_string()),
            },
            &identity,
        )
        .unwrap();

        let k_v1 = build_cache_key(
            "wh",
            &Action::GetManifest {
                namespace: Namespace::new("library/nginx").unwrap(),
                reference: Reference::Tag("v1.0.0".to_string()),
            },
            &identity,
        )
        .unwrap();

        assert_ne!(k_latest, k_v1);
    }
}
