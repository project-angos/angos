use std::str::FromStr;

use http::{HeaderMap, HeaderName, HeaderValue, request::Parts};
use serde_json::Value;

use crate::{
    auth::Error,
    identity::{Action, ClientIdentity, RequestScheme},
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
static X_REGISTRY_OIDC_PROVIDER: &str = "X-Registry-OIDC-Provider";
static X_REGISTRY_OIDC_SUBJECT: &str = "X-Registry-OIDC-Subject";

pub fn build_header_name(name: &str) -> Result<HeaderName, Error> {
    HeaderName::from_str(name)
        .map_err(|e| Error::Execution(format!("Invalid header name '{name}': {e}")))
}

pub fn build_header_value(value: &str) -> Result<HeaderValue, Error> {
    HeaderValue::from_str(value)
        .map_err(|e| Error::Execution(format!("Invalid header value '{value}': {e}")))
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
    let proto = match parts.extensions.get::<RequestScheme>() {
        Some(scheme) => scheme.as_str(),
        None if parts.uri.scheme_str() == Some("https") => "https",
        None => "http",
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
    // Without these an OIDC caller reaches the webhook anonymous, so it could
    // not tell one OIDC user from another.
    if let Some(oidc) = &identity.oidc {
        headers.insert(
            X_REGISTRY_OIDC_PROVIDER,
            build_header_value(&oidc.provider_name)?,
        );
        if let Some(subject) = oidc.claims.get("sub").and_then(Value::as_str) {
            headers.insert(X_REGISTRY_OIDC_SUBJECT, build_header_value(subject)?);
        }
    }

    // Operator-selected client headers, forwarded verbatim. A repeated header
    // carries every one of its values.
    for name in forward_headers {
        for value in parts.headers.get_all(name) {
            headers.append(build_header_name(name)?, value.clone());
        }
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use http::{Request, request::Parts};
    use serde_json::json;

    use angos_oci::{Namespace, Reference, Tag};

    use crate::auth::webhook::headers::build_headers;
    use crate::identity::{Action, ClientIdentity, OidcClaims, RequestScheme};

    fn anonymous() -> ClientIdentity {
        ClientIdentity::new(None)
    }

    fn identity_with_oidc(provider_name: &str, subject: &str) -> ClientIdentity {
        let mut id = ClientIdentity::new(None);
        id.oidc = Some(OidcClaims {
            provider_name: provider_name.to_string(),
            claims: HashMap::from([("sub".to_string(), json!(subject))]),
        });
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

    fn get_manifest(tag: &str) -> Action {
        Action::GetManifest {
            namespace: Namespace::new("library/nginx").unwrap(),
            reference: Reference::Tag(Tag::new(tag).unwrap()),
        }
    }

    #[test]
    fn x_forwarded_proto_follows_the_resolved_scheme_extension() {
        // The connection handler resolves a trusted proxy's scheme into the
        // extension; the webhook must forward that, not the listener's raw one.
        let mut parts = parts_with_headers(&[]);
        parts.extensions.insert(RequestScheme::Https);
        let headers = build_headers(&[], &get_manifest("v1"), &anonymous(), &parts).unwrap();
        assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "https");
    }

    #[test]
    fn every_value_of_a_repeated_forwarded_header_is_kept() {
        let headers = build_headers(
            &["X-Tenant".to_string()],
            &Action::ApiVersion,
            &anonymous(),
            &parts_with_headers(&[("X-Tenant", "tenant-a"), ("X-Tenant", "tenant-b")]),
        )
        .unwrap();

        let values: Vec<&str> = headers
            .get_all("X-Tenant")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect();
        assert_eq!(
            values,
            ["tenant-a", "tenant-b"],
            "the webhook must decide on every value the client sent"
        );
    }

    #[test]
    fn an_oidc_identity_reaches_the_webhook() {
        let headers = build_headers(
            &[],
            &Action::ApiVersion,
            &identity_with_oidc("github-actions", "repo:myorg/myapp:ref:refs/heads/main"),
            &parts_with_headers(&[]),
        )
        .unwrap();

        assert_eq!(
            headers.get("X-Registry-OIDC-Provider").unwrap(),
            "github-actions"
        );
        assert_eq!(
            headers.get("X-Registry-OIDC-Subject").unwrap(),
            "repo:myorg/myapp:ref:refs/heads/main"
        );
    }

    /// A provider is free to omit `sub`, which must leave the caller identified
    /// by provider alone rather than failing the request.
    #[test]
    fn an_oidc_identity_without_a_subject_still_names_its_provider() {
        let mut identity = identity_with_oidc("gh", "alice");
        if let Some(oidc) = identity.oidc.as_mut() {
            oidc.claims.remove("sub");
        }

        let headers = build_headers(
            &[],
            &Action::ApiVersion,
            &identity,
            &parts_with_headers(&[]),
        )
        .unwrap();

        assert_eq!(headers.get("X-Registry-OIDC-Provider").unwrap(), "gh");
        assert!(headers.get("X-Registry-OIDC-Subject").is_none());
    }
}
