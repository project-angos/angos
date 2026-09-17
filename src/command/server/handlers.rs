//! The endpoints the registry does not serve: token exchange, the embedded web
//! UI, and the operational probes. Everything OCI answers from `registry`.

use std::{borrow::Cow, fmt::Display};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    HeaderMap, Response, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE, HeaderValue, X_CONTENT_TYPE_OPTIONS, X_FRAME_OPTIONS},
};
use rust_embed::Embed;
use serde::Serialize;
use tracing::warn;

use angos_transport::{ResponseBody, build_response, json_headers, json_response};

use crate::{
    auth::TokenIssuer, command::server::error::Error, identity::ClientIdentity,
    metrics_provider::metrics_provider, registry::Registry,
};

/// The response field names are the ones OCI clients read.
#[derive(Serialize)]
pub struct TokenBody {
    token: String,
    expires_in: u64,
}

#[derive(Clone, Serialize)]
pub struct UiConfigBody {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc: Option<UiOidcBody>,
}

/// What a browser needs to run the authorization code flow itself. Every field
/// is public by nature: the client is a public one, holding no secret.
#[derive(Clone, Serialize)]
pub struct UiOidcBody {
    pub issuer: String,
    pub client_id: String,
    pub scopes: String,
}

#[derive(Serialize)]
pub struct StatusBody {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Embed)]
#[folder = "ui/build"]
struct UiAssets;

/// Exchanges the credential that authenticated this request for a
/// registry-issued token.
///
/// A registry token is not such a credential: renewing one would let a token
/// outlive the credential it was minted from forever, and `ttl_secs` would
/// bound nothing.
pub fn handle_get_token(
    token_issuer: &TokenIssuer,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    if identity.from_registry_token {
        return Err(Error::Unauthorized(
            "A registry token cannot be exchanged for another".to_string(),
        ));
    }

    let (token, expires_in) = token_issuer.issue(identity)?;
    let body = TokenBody { token, expires_in };
    // The body is a bearer credential, so no shared cache may store it and hand
    // it to the next client asking for one.
    let mut headers = json_headers();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));

    Ok(build_response(
        StatusCode::OK,
        headers,
        ResponseBody::fixed(serde_json::to_vec(&body)?),
    )?)
}

pub fn handle_ui_config(ui_config: &UiConfigBody) -> Result<Response<ResponseBody>, Error> {
    json_response(StatusCode::OK, ui_config)
}

pub fn handle_ui_asset(path: &str) -> Result<Response<ResponseBody>, Error> {
    let asset_path = path.trim_start_matches('/');
    let asset_path = if asset_path.is_empty() {
        "index.html"
    } else {
        asset_path
    };

    if let Some(content) = UiAssets::get(asset_path) {
        let mime = mime_guess::from_path(asset_path).first_or_octet_stream();
        return asset_response(mime.as_ref(), content.data);
    }

    if let Some(content) = UiAssets::get("index.html") {
        return asset_response("text/html; charset=utf-8", content.data);
    }

    Err(Error::NotFound("UI asset not found".to_string()))
}

fn asset_response(mime: &str, data: Cow<'static, [u8]>) -> Result<Response<ResponseBody>, Error> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::try_from(mime)?);
    // The SPA fallback answers any unknown path with the session-bearing HTML,
    // so pin its content type and forbid framing it.
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));

    Ok(build_response(
        StatusCode::OK,
        headers,
        ResponseBody::Fixed(Full::new(asset_bytes(data))),
    )?)
}

/// Wrap an embedded asset's bytes without copying them. A release build embeds
/// the assets, so the `Cow` borrows `'static` data every request shares; a debug
/// build reads each one from disk, and the `Bytes` takes that buffer over.
fn asset_bytes(data: Cow<'static, [u8]>) -> Bytes {
    match data {
        Cow::Borrowed(embedded) => Bytes::from_static(embedded),
        Cow::Owned(read_from_disk) => Bytes::from(read_from_disk),
    }
}

pub fn handle_healthz() -> Result<Response<ResponseBody>, Error> {
    let body = StatusBody {
        status: "ok",
        error: None,
    };

    json_response(StatusCode::OK, &body)
}

/// A backend that cannot be listed is reported as `503 not_ready`. The cause
/// is logged, not returned: `/readyz` is exposed anonymously, and a storage
/// error names the bucket and endpoint.
pub async fn handle_readyz(registry: &Registry) -> Result<Response<ResponseBody>, Error> {
    let (status, body) = match registry.check_ready().await {
        Ok(()) => (
            StatusCode::OK,
            StatusBody {
                status: "ready",
                error: None,
            },
        ),
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, not_ready_body(&error)),
    };

    json_response(status, &body)
}

/// The `not_ready` body. The cause is logged, never returned: a storage error
/// names the bucket and endpoint, and `/readyz` is exposed anonymously.
fn not_ready_body(error: &impl Display) -> StatusBody {
    warn!("readiness probe failed: {error}");
    StatusBody {
        status: "not_ready",
        error: Some("storage backend not ready".to_string()),
    }
}

pub fn handle_metrics() -> Result<Response<ResponseBody>, Error> {
    let (content_type, metrics) = metrics_provider().gather()?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::try_from(content_type)?);

    Ok(build_response(
        StatusCode::OK,
        headers,
        ResponseBody::fixed(metrics),
    )?)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use bytes::Bytes;

    use super::{asset_bytes, asset_response, not_ready_body};

    /// The embedded (release) shape: every request must share the one `'static`
    /// copy rather than allocating its own, which pointer identity is the only
    /// direct evidence of.
    #[test]
    fn borrowed_asset_is_served_without_copying() {
        static EMBEDDED: &[u8] = b"<!doctype html>";

        let served = asset_bytes(Cow::Borrowed(EMBEDDED));

        assert_eq!(served, EMBEDDED);
        assert_eq!(
            served.as_ptr(),
            EMBEDDED.as_ptr(),
            "an embedded asset must be served from its own storage, not a copy"
        );
    }

    /// The read-from-disk (debug) shape: the buffer is moved into the `Bytes`.
    #[test]
    fn owned_asset_keeps_its_buffer() {
        let read_from_disk = b"<!doctype html>".to_vec();
        let address = read_from_disk.as_ptr();

        let served = asset_bytes(Cow::Owned(read_from_disk));

        assert_eq!(served, Bytes::from_static(b"<!doctype html>"));
        assert_eq!(
            served.as_ptr(),
            address,
            "an asset read from disk must be handed over, not copied"
        );
    }

    /// `/readyz` is exposed anonymously, so its body must not relay the storage
    /// error, which names the bucket and endpoint.
    #[test]
    fn not_ready_body_hides_the_backend_error() {
        let leaky = "list failed: endpoint=http://s3.internal:9000 bucket=secret-bucket";
        let body = not_ready_body(&leaky);
        assert_eq!(body.status, "not_ready");
        assert_eq!(body.error.as_deref(), Some("storage backend not ready"));
    }

    /// The SPA HTML holds the registry session, so it must carry the sniffing
    /// and framing guards on every asset response.
    #[test]
    fn asset_response_carries_security_headers() {
        let response = asset_response(
            "text/html; charset=utf-8",
            Cow::Borrowed(b"<!doctype html>"),
        )
        .unwrap();
        let headers = response.headers();
        assert_eq!(headers.get("X-Content-Type-Options").unwrap(), "nosniff");
        assert_eq!(headers.get("X-Frame-Options").unwrap(), "DENY");
    }
}
