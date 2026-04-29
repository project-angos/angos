use std::str::FromStr;

use hyper::{
    header::{HeaderName, HeaderValue},
    http::{HeaderMap, request::Parts},
};

use crate::{
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
        let value = build_header_value(reference.as_str())?;
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
    let Ok(action_json) = serde_json::to_string(action) else {
        let msg = "Failed to serialize action".to_string();
        return Err(Error::Execution(msg));
    };

    let Ok(identity_json) = serde_json::to_string(&identity) else {
        let msg = "Failed to serialize identity".to_string();
        return Err(Error::Execution(msg));
    };

    Ok(format!("webhook:{name}:{identity_json}:{action_json}"))
}
