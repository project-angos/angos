use http::request::Parts;
use http::{HeaderMap, HeaderValue};
use serde_json::Value;

use crate::auth::Error;
use crate::auth::webhook::config::ForwardHeader;
use crate::identity::{Action, ClientIdentity, RequestScheme};

pub fn build_header_value(value: &str) -> Result<HeaderValue, Error> {
    HeaderValue::from_str(value)
        .map_err(|e| Error::Execution(format!("Invalid header value '{value}': {e}")))
}

pub fn build_headers(
    forward_headers: &[ForwardHeader],
    action: &Action,
    identity: &ClientIdentity,
    parts: &Parts,
) -> Result<HeaderMap, Error> {
    let mut headers = HeaderMap::new();

    // Forwarded request context.
    headers.insert(
        "X-Forwarded-Method",
        build_header_value(parts.method.as_str())?,
    );
    let proto = match parts.extensions.get::<RequestScheme>() {
        Some(scheme) => scheme.as_str(),
        None if parts.uri.scheme_str() == Some("https") => "https",
        None => "http",
    };
    headers.insert("X-Forwarded-Proto", build_header_value(proto)?);
    if let Some(host) = parts.headers.get("Host") {
        headers.insert("X-Forwarded-Host", host.clone());
    }
    headers.insert(
        "X-Forwarded-Uri",
        build_header_value(&parts.uri.to_string())?,
    );
    if let Some(ip) = &identity.client_ip {
        headers.insert("X-Forwarded-For", build_header_value(ip)?);
    }

    // The registry action under authorization.
    headers.insert(
        "X-Registry-Action",
        build_header_value(action.action_name())?,
    );
    if let Some(namespace) = action.namespace() {
        headers.insert("X-Registry-Namespace", build_header_value(namespace)?);
    }
    if let Some(reference) = action.reference() {
        headers.insert(
            "X-Registry-Reference",
            build_header_value(&reference.to_string())?,
        );
    }
    if let Some(digest) = action.digest() {
        headers.insert(
            "X-Registry-Digest",
            build_header_value(&digest.to_string())?,
        );
    }

    // The caller's identity.
    if let Some(username) = &identity.username {
        headers.insert("X-Registry-Username", build_header_value(username)?);
    }
    if let Some(id) = &identity.id {
        headers.insert("X-Registry-Identity-ID", build_header_value(id)?);
    }
    for cn in &identity.certificate.common_names {
        headers.append("X-Registry-Certificate-CN", build_header_value(cn)?);
    }
    for org in &identity.certificate.organizations {
        headers.append("X-Registry-Certificate-O", build_header_value(org)?);
    }
    // Without these an OIDC caller reaches the webhook anonymous, so it could
    // not tell one OIDC user from another.
    if let Some(oidc) = &identity.oidc {
        headers.insert(
            "X-Registry-OIDC-Provider",
            build_header_value(&oidc.provider_name)?,
        );
        if let Some(subject) = oidc.claims.get("sub").and_then(Value::as_str) {
            headers.insert("X-Registry-OIDC-Subject", build_header_value(subject)?);
        }
    }

    // Operator-selected client headers, forwarded verbatim. A repeated header
    // carries every one of its values.
    for header in forward_headers {
        for value in parts.headers.get_all(header.name()) {
            headers.append(header.name().clone(), value.clone());
        }
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use http::Request;
    use http::request::Parts;
    use serde_json::json;

    use angos_oci::{Namespace, Reference, Tag};

    use crate::auth::webhook::config::ForwardHeader;
    use crate::auth::webhook::headers::build_headers;
    use crate::identity::{Action, ClientIdentity, OidcClaims, RequestScheme};

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
        let headers =
            build_headers(&[], &get_manifest("v1"), &ClientIdentity::new(None), &parts).unwrap();
        assert_eq!(headers.get("X-Forwarded-Proto").unwrap(), "https");
    }

    #[test]
    fn every_value_of_a_repeated_forwarded_header_is_kept() {
        let headers = build_headers(
            &[ForwardHeader::try_from("X-Tenant".to_string()).unwrap()],
            &Action::ApiVersion,
            &ClientIdentity::new(None),
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

    /// A provider is free to omit `sub`, which leaves the caller identified by
    /// provider alone rather than failing the request.
    #[test]
    fn an_oidc_identity_reaches_the_webhook() {
        for subject in [Some("repo:myorg/myapp:ref:refs/heads/main"), None] {
            let mut identity = ClientIdentity::new(None);
            identity.oidc = Some(OidcClaims {
                provider_name: "github-actions".to_string(),
                claims: subject
                    .map(|s| ("sub".to_string(), json!(s)))
                    .into_iter()
                    .collect(),
            });

            let headers = build_headers(
                &[],
                &Action::ApiVersion,
                &identity,
                &parts_with_headers(&[]),
            )
            .unwrap();

            assert_eq!(
                headers.get("X-Registry-OIDC-Provider").unwrap(),
                "github-actions"
            );
            assert_eq!(
                headers
                    .get("X-Registry-OIDC-Subject")
                    .map(|v| v.to_str().unwrap()),
                subject
            );
        }
    }
}
