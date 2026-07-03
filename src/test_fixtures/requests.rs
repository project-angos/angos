//! Builders for the request `Parts` handed to auth middlewares under test.

use base64::{Engine, prelude::BASE64_STANDARD};
use hyper::{Request, header::AUTHORIZATION, http::request::Parts};

/// Parts of a bare GET request with no headers.
///
/// # Panics
///
/// Panics if the request cannot be built.
pub fn empty_parts() -> Parts {
    Request::builder().body(()).unwrap().into_parts().0
}

/// Parts of a bare GET request targeting `uri`.
///
/// # Panics
///
/// Panics if `uri` is not a valid URI.
pub fn parts_with_uri(uri: &str) -> Parts {
    Request::builder().uri(uri).body(()).unwrap().into_parts().0
}

/// Parts carrying a raw `Authorization` header value.
///
/// # Panics
///
/// Panics if `value` is not a valid header value.
pub fn parts_with_authorization(value: &str) -> Parts {
    Request::builder()
        .header(AUTHORIZATION, value)
        .body(())
        .unwrap()
        .into_parts()
        .0
}

/// Parts carrying `Authorization: Basic base64(username:password)`.
pub fn parts_with_basic_auth(username: &str, password: &str) -> Parts {
    let credentials = BASE64_STANDARD.encode(format!("{username}:{password}"));
    parts_with_authorization(&format!("Basic {credentials}"))
}
