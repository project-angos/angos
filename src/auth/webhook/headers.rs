use std::str::FromStr;

use hyper::{
    header::{HeaderName, HeaderValue},
    http::{HeaderMap, request::Parts},
};

use crate::{
    auth::Error,
    auth::sha256_hex,
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

pub fn build_headers(
    forward_headers: &[String],
    action: &Action,
    identity: &ClientIdentity,
    parts: &Parts,
) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::new();

    // Forwarded request context.
    headers.insert(
        X_FORWARDED_METHOD,
        build_header_value(parts.method.as_str())?,
    );
    let proto = if parts.uri.scheme_str() == Some("https") {
        "https"
    } else {
        "http"
    };
    headers.insert(X_FORWARDED_PROTO, build_header_value(proto)?);
    if let Some(host) = parts.headers.get("Host") {
        headers.insert(X_FORWARDED_HOST, host.clone());
    }
    headers.insert(X_FORWARDED_URI, build_header_value(&parts.uri.to_string())?);
    if let Some(ip) = &identity.client_ip {
        headers.insert(X_FORWARDED_FOR, build_header_value(ip)?);
    }

    // The registry action under authorization.
    headers.insert(X_REGISTRY_ACTION, build_header_value(action.action_name())?);
    if let Some(namespace) = action.get_namespace() {
        headers.insert(X_REGISTRY_NAMESPACE, build_header_value(namespace)?);
    }
    if let Some(reference) = action.get_reference() {
        headers.insert(
            X_REGISTRY_REFERENCE,
            build_header_value(&reference.to_string())?,
        );
    }
    if let Some(digest) = action.get_digest() {
        headers.insert(X_REGISTRY_DIGEST, build_header_value(&digest.to_string())?);
    }

    // The caller's identity.
    if let Some(username) = &identity.username {
        headers.insert(X_REGISTRY_USERNAME, build_header_value(username)?);
    }
    if let Some(id) = &identity.id {
        headers.insert(X_REGISTRY_IDENTITY_ID, build_header_value(id)?);
    }
    for cn in &identity.certificate.common_names {
        headers.append(X_REGISTRY_CERTIFICATE_CN, build_header_value(cn)?);
    }
    for org in &identity.certificate.organizations {
        headers.append(X_REGISTRY_CERTIFICATE_O, build_header_value(org)?);
    }

    // Operator-selected client headers, forwarded verbatim.
    for name in forward_headers {
        if let Some(value) = parts.headers.get(name) {
            headers.insert(build_header_name(name)?, value.clone());
        }
    }

    Ok(headers)
}

/// Cache key for a webhook decision: a digest over the exact set of forwarded
/// request headers. Keying off the forwarded context (not just identity and
/// action) means the key covers everything the webhook can decide on, so a
/// cached allow can never be replayed across a different host, URI, or
/// operator-forwarded header. Entries are length-prefixed and sorted for a
/// canonical, order-independent digest.
pub fn build_cache_key(name: &str, headers: &HeaderMap) -> String {
    let mut entries: Vec<(&[u8], &[u8])> = headers
        .iter()
        .map(|(header, value)| (header.as_str().as_bytes(), value.as_bytes()))
        .collect();
    entries.sort_unstable();

    let mut material = Vec::new();
    for (header, value) in entries {
        material.extend_from_slice(&(header.len() as u64).to_le_bytes());
        material.extend_from_slice(header);
        material.extend_from_slice(&(value.len() as u64).to_le_bytes());
        material.extend_from_slice(value);
    }
    format!("webhook:{name}:{}", sha256_hex(material))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use hyper::{
        Request,
        http::{HeaderMap, HeaderName, HeaderValue, request::Parts},
    };

    use super::{build_cache_key, build_headers};
    use crate::{
        identity::{Action, ClientIdentity},
        oci::{Namespace, Reference, Tag},
    };

    fn anonymous() -> ClientIdentity {
        ClientIdentity::new(None)
    }

    fn identity_with_username(username: &str) -> ClientIdentity {
        let mut id = ClientIdentity::new(None);
        id.username = Some(username.to_string());
        id
    }

    /// A bare GET `Parts` carrying the given request headers.
    fn parts_with_headers(headers: &[(&str, &str)]) -> Parts {
        let mut builder = Request::builder();
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(()).unwrap().into_parts().0
    }

    /// Cache key as the authorizer computes it: a digest of the headers
    /// forwarded to the webhook.
    fn key_for(
        name: &str,
        forward: &[String],
        action: &Action,
        identity: &ClientIdentity,
        parts: &Parts,
    ) -> String {
        build_cache_key(
            name,
            &build_headers(forward, action, identity, parts).unwrap(),
        )
    }

    fn simple_key(name: &str, action: &Action, identity: &ClientIdentity) -> String {
        key_for(name, &[], action, identity, &parts_with_headers(&[]))
    }

    fn get_manifest(tag: &str) -> Action {
        Action::GetManifest {
            namespace: Namespace::new("library/nginx").unwrap(),
            reference: Reference::Tag(Tag::new(tag).unwrap()),
        }
    }

    #[test]
    fn same_inputs_produce_same_key() {
        let action = Action::ApiVersion;
        let identity = anonymous();
        assert_eq!(
            simple_key("wh", &action, &identity),
            simple_key("wh", &action, &identity)
        );
    }

    #[test]
    fn different_action_produces_different_key() {
        let identity = anonymous();
        assert_ne!(
            simple_key("wh", &Action::ApiVersion, &identity),
            simple_key("wh", &get_manifest("latest"), &identity)
        );
    }

    #[test]
    fn different_identity_produces_different_key() {
        let action = Action::ApiVersion;
        assert_ne!(
            simple_key("wh", &action, &anonymous()),
            simple_key("wh", &action, &identity_with_username("alice"))
        );
    }

    #[test]
    fn different_webhook_name_produces_different_key() {
        let action = Action::ApiVersion;
        let identity = anonymous();
        assert_ne!(
            simple_key("webhook-a", &action, &identity),
            simple_key("webhook-b", &action, &identity)
        );
    }

    #[test]
    fn key_contains_webhook_prefix_and_name() {
        let key = simple_key("my-hook", &Action::ApiVersion, &anonymous());
        assert!(
            key.starts_with("webhook:my-hook:"),
            "key must be prefixed with 'webhook:<name>:': {key}"
        );
    }

    #[test]
    fn key_uses_bounded_sha256_digest() {
        let key = simple_key("my-hook", &Action::ApiVersion, &anonymous());
        let digest = key.strip_prefix("webhook:my-hook:").unwrap();
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn key_does_not_expose_forwarded_values_in_plaintext() {
        // The key is a digest, so a sensitive forwarded value never appears
        // verbatim in it.
        let key = key_for(
            "my-hook",
            &["X-Secret".to_string()],
            &Action::ApiVersion,
            &identity_with_username("secret-user"),
            &parts_with_headers(&[("X-Secret", "topsecret-value")]),
        );
        assert!(!key.contains("secret-user"), "key was: {key}");
        assert!(!key.contains("topsecret-value"), "key was: {key}");
    }

    #[test]
    fn different_reference_same_namespace_produces_different_key() {
        let identity = anonymous();
        assert_ne!(
            simple_key("wh", &get_manifest("latest"), &identity),
            simple_key("wh", &get_manifest("v1.0.0"), &identity)
        );
    }

    // Regression guards for M1: a cached decision must not replay across a
    // forwarded context the webhook could have decided on differently.

    #[test]
    fn different_forwarded_host_produces_different_key() {
        let action = Action::ApiVersion;
        let identity = anonymous();
        assert_ne!(
            key_for(
                "wh",
                &[],
                &action,
                &identity,
                &parts_with_headers(&[("Host", "a.example")])
            ),
            key_for(
                "wh",
                &[],
                &action,
                &identity,
                &parts_with_headers(&[("Host", "b.example")])
            ),
            "a cached allow must not replay across a different Host"
        );
    }

    #[test]
    fn different_forwarded_header_value_produces_different_key() {
        let action = Action::ApiVersion;
        let identity = anonymous();
        let forward = ["X-Tenant".to_string()];
        assert_ne!(
            key_for(
                "wh",
                &forward,
                &action,
                &identity,
                &parts_with_headers(&[("X-Tenant", "tenant-a")])
            ),
            key_for(
                "wh",
                &forward,
                &action,
                &identity,
                &parts_with_headers(&[("X-Tenant", "tenant-b")])
            ),
            "a cached allow must not replay across a different forwarded header"
        );
    }

    #[test]
    fn key_is_independent_of_header_insertion_order() {
        let build = |pairs: &[(&str, &str)]| {
            let mut map = HeaderMap::new();
            for (name, value) in pairs {
                map.append(
                    HeaderName::from_str(name).unwrap(),
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            build_cache_key("wh", &map)
        };
        assert_eq!(
            build(&[("a-header", "1"), ("b-header", "2")]),
            build(&[("b-header", "2"), ("a-header", "1")])
        );
    }
}
