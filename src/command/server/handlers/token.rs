use hyper::{
    Response, StatusCode,
    header::{CACHE_CONTROL, HeaderValue},
};
use serde::Serialize;

use crate::{
    command::server::{
        ServerContext, error::Error, handlers::json_response, response_body::ResponseBody,
    },
    identity::ClientIdentity,
};

/// Exchanges the credential that authenticated this request for a registry-issued
/// token. The response field names are the ones OCI clients read.
///
/// A registry token is not such a credential: renewing one would let a token
/// outlive the credential it was minted from forever, and `ttl_secs` would bound
/// nothing.
pub fn handle_get_token(
    context: &ServerContext,
    identity: &ClientIdentity,
) -> Result<Response<ResponseBody>, Error> {
    #[derive(Serialize)]
    struct TokenResponse {
        token: String,
        expires_in: u64,
    }

    let Some(token_issuer) = context.token_issuer() else {
        return Err(Error::NotFound(
            "No token service is configured".to_string(),
        ));
    };

    if identity.from_registry_token {
        return Err(Error::Unauthorized(
            "A registry token cannot be exchanged for another".to_string(),
        ));
    }

    let (token, expires_in) = token_issuer.issue(identity)?;
    let mut response = json_response(StatusCode::OK, &TokenResponse { token, expires_in })?;
    // The body is a bearer credential, so no shared cache may store it and hand
    // it to the next client asking for one.
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}
