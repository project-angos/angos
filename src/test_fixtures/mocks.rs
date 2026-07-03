//! Shared wiremock responders for the mock shapes tests mount repeatedly:
//! the OIDC JWKS endpoint and the downstream registry blob API.

use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::{
    oci::Digest,
    registry::DOCKER_CONTENT_DIGEST,
    test_fixtures::oidc::{KID, jwk_x, jwk_y},
};

/// The JWKS document advertising the `test_fixtures::oidc` primary key.
pub fn static_jwks_response() -> Value {
    json!({
        "keys": [{
            "kty": "EC",
            "use": "sig",
            "kid": KID,
            "crv": "P-256",
            "x": jwk_x(),
            "y": jwk_y(),
            "alg": "ES256"
        }]
    })
}

/// Serves `jwks` with a 200 at the conventional `/.well-known/jwks` path.
pub async fn mount_jwks(server: &MockServer, jwks: Value) {
    Mock::given(method("GET"))
        .and(path("/.well-known/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(jwks))
        .mount(server)
        .await;
}

/// A downstream registry accepting a full upload of `blobs` under
/// `namespace`: each blob HEAD-misses exactly once (404), then the
/// POST/PATCH/PUT session accepts one upload per blob. The counts are
/// asserted when the `MockServer` drops.
pub async fn mount_blob_upload_accepted(server: &MockServer, namespace: &str, blobs: &[&Digest]) {
    for blob in blobs {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{namespace}/blobs/{blob}")))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(server)
            .await;
    }
    let uploads = blobs.len() as u64;
    Mock::given(method("POST"))
        .and(path(format!("/v2/{namespace}/blobs/uploads/")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{namespace}/blobs/uploads/s1")),
        )
        .expect(uploads)
        .mount(server)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/v2/{namespace}/blobs/uploads/s1")))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", format!("/v2/{namespace}/blobs/uploads/s1")),
        )
        .expect(uploads)
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/v2/{namespace}/blobs/uploads/s1")))
        .respond_with(ResponseTemplate::new(201))
        .expect(uploads)
        .mount(server)
        .await;
}

/// Mounts a HEAD per blob returning 200 (already present downstream) so a
/// push skips the upload sequence.
pub async fn mount_blobs_present(server: &MockServer, namespace: &str, blobs: &[&Digest]) {
    for blob in blobs {
        Mock::given(method("HEAD"))
            .and(path(format!("/v2/{namespace}/blobs/{blob}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(DOCKER_CONTENT_DIGEST, blob.to_string().as_str())
                    .insert_header("Content-Length", "10"),
            )
            .mount(server)
            .await;
    }
}
