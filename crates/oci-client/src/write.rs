//! Write methods for [`RegistryClient`].
//!
//! Byte-bodied requests reuse [`RegistryClient::send_with_auth_retry`]'s
//! cached-token-then-single-refresh orchestration. The single-use stream in
//! [`RegistryClient::patch_upload`] cannot be replayed, so it reuses the auth
//! header resolved when the session was opened ([`UploadSession::auth`]) and
//! surfaces a `401` as an error instead of retrying.

use bytes::Bytes;
use reqwest::{
    Body, Method, Response, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION},
};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use tracing::{debug, instrument, warn};

use angos_oci::client::{self, append_query};
use angos_oci::header::{DOCKER_CONTENT_DIGEST, OCI_SUBJECT};
use angos_oci::request::{
    DeleteManifestRequest, MountBlobRequest, PutManifestRequest, StartUploadRequest,
};
use angos_oci::response::{DeleteManifestOutcome, ErrorResponse, PutManifestOutcome};
use angos_oci::{Digest, MediaType};

use crate::{
    Error, REPLICATION_SUPERSEDED_CODE, RegistryClient, X_ANGOS_SOURCE_TIMESTAMP, parse_header,
    without_query,
};

/// An open downstream blob-upload session: the server-assigned continuation
/// URL plus the auth header that opened it.
///
/// The streamed `PATCH` reuses [`Self::auth`] because the session URL never
/// issues its own auth challenge and a single-use body cannot be replayed to
/// refresh a token on a `401`. Not `Debug`: `auth` carries a bearer/basic
/// credential.
pub struct UploadSession {
    pub url: String,
    pub auth: Option<String>,
}

/// Returns the first OCI error `code` from a `{"errors":[{"code":"..."}]}`
/// response body, or `None` when the body is malformed or absent.
async fn parse_oci_error_code(response: Response) -> Option<String> {
    let bytes = response.bytes().await.ok()?;
    serde_json::from_slice::<ErrorResponse>(&bytes)
        .ok()?
        .first_code()
        .map(ToString::to_string)
}

/// The digest a write response advertises. `Ok(None)` means the remote sent no
/// `Docker-Content-Digest`, which the OCI spec states as optional. A header
/// that is present but not a digest is a protocol fault the caller reports,
/// never an absent one, so the checks fed by this value cannot be skipped by
/// sending garbage.
fn advertised_digest(response: &Response) -> Result<Option<Digest>, Error> {
    if !response.headers().contains_key(DOCKER_CONTENT_DIGEST) {
        return Ok(None);
    }
    parse_header(response, DOCKER_CONTENT_DIGEST).map(Some)
}

/// Classifies a failed (non-2xx) write response. A `403` is a terminal
/// authorization denial that no retry can clear; anything else is a retryable
/// failure. Mirrors the `send_stream` mapping so every write path agrees.
fn write_failure(op: &str, status: StatusCode) -> Error {
    if status == StatusCode::FORBIDDEN {
        Error::Denied(format!("{op} rejected with 403"))
    } else {
        Error::Internal(format!("{op} failed with status {status}"))
    }
}

impl RegistryClient {
    /// Sends a replayable body under the auth retry, also returning the
    /// resolved auth header so an upload-session opener can reuse it for the
    /// single-use streamed `PATCH`.
    async fn send_body_with_auth(
        &self,
        method: &Method,
        location: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
        source_ts: Option<&str>,
    ) -> Result<(Response, Option<String>), Error> {
        debug!("Writing to upstream: {method} {}", without_query(location));

        // `Bytes` makes the per-attempt clone a refcount bump, not a deep copy.
        let body = Bytes::from(body);

        self.send_with_auth_retry_capturing(location, |auth| {
            // The closure may run twice (cached token, then 401 refresh), so
            // clone the refcounted body per attempt.
            let body = body.clone();
            async move {
                self.send_body_once(
                    method,
                    location,
                    content_type,
                    body,
                    auth.as_deref(),
                    source_ts,
                )
                .await
            }
        })
        .await
    }

    async fn send_body_once(
        &self,
        method: &Method,
        location: &str,
        content_type: Option<&str>,
        body: Bytes,
        auth_header: Option<&str>,
        source_ts: Option<&str>,
    ) -> Result<Response, Error> {
        let mut request = self
            .build_request(method, &[], location, auth_header, None)
            .body(body);
        if let Some(content_type) = content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        if let Some(source_ts) = source_ts {
            request = request.header(X_ANGOS_SOURCE_TIMESTAMP, source_ts);
        }
        request
            .send()
            .await
            .map_err(|e| Error::Internal(format!("HTTP request failed: {e}")))
    }

    /// Sends a request whose body is a single-use stream.
    ///
    /// The consumed stream cannot be replayed, so a `401` is surfaced as an
    /// error instead of a token-refresh retry. `auth_header` is the credential
    /// resolved when the session was opened; a cached lookup keyed on
    /// `location` is the anonymous-or-direct-call fallback.
    async fn send_stream<S>(
        &self,
        method: &Method,
        location: &str,
        auth_header: Option<&str>,
        content_length: u64,
        stream: S,
    ) -> Result<Response, Error>
    where
        S: AsyncRead + Unpin + Send + Sync + 'static,
    {
        debug!(
            "Streaming to upstream: {method} {}",
            without_query(location)
        );

        let auth = match auth_header {
            Some(header) => Some(header.to_string()),
            None => self.cached_auth_header(location).await,
        };
        let body = Body::wrap_stream(ReaderStream::new(stream));
        let response = self
            .build_request(method, &[], location, auth.as_deref(), None)
            .header(CONTENT_TYPE, "application/octet-stream")
            .header(CONTENT_LENGTH, content_length)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("HTTP request failed: {e}")))?;

        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(Error::Unauthorized(
                "upload stream rejected with 401 (token cannot be replayed for a consumed stream)"
                    .to_string(),
            ));
        }
        if response.status() == StatusCode::FORBIDDEN {
            return Err(Error::Denied("Access forbidden".to_string()));
        }

        Ok(response)
    }

    /// Reads the `Location` response header, resolving a relative value (which
    /// OCI registries may return) against the response's final URL.
    fn parse_location(response: &Response) -> Result<String, Error> {
        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| {
                Error::Internal("upstream response is missing Location header".into())
            })?;

        let base = response.url();
        let joined = base
            .join(location)
            .map_err(|e| Error::Internal(format!("invalid Location header '{location}': {e}")))?;
        // Following a cross-origin Location would carry the session's auth to
        // a host the registry named, so a hostile upstream could harvest it.
        if joined.origin() != base.origin() {
            return Err(Error::Denied(format!(
                "upstream Location '{joined}' points to a different origin than {base}"
            )));
        }
        Ok(joined.to_string())
    }

    /// Starts a resumable blob upload session, returning the server-assigned
    /// continuation URL from the `Location` header and the auth header that
    /// opened it (reused for the streamed `PATCH`).
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, access is rejected, or the
    /// response lacks a success status or `Location` header.
    #[instrument(skip(self))]
    pub async fn start_upload(&self, request: StartUploadRequest) -> Result<UploadSession, Error> {
        let location = client::uploads_start_path(&self.url, &request.namespace);
        let (response, auth) = self
            .send_body_with_auth(&Method::POST, &location, None, Vec::new(), None)
            .await?;

        if !response.status().is_success() {
            return Err(write_failure("start_upload", response.status()));
        }

        Ok(UploadSession {
            url: Self::parse_location(&response)?,
            auth,
        })
    }

    /// Attempts an OCI cross-repository blob mount
    /// (`?mount=<digest>[&from=<repo>]`).
    ///
    /// `Ok(None)` means mounted (`201`, with the advertised digest verified
    /// when present); `Ok(Some(session))` means the mount degraded to a normal
    /// upload session (`202`), per the distribution spec.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects the mount,
    /// or a session response lacks a success status or `Location` header.
    #[instrument(skip(self))]
    pub async fn mount_blob(
        &self,
        request: MountBlobRequest,
    ) -> Result<Option<UploadSession>, Error> {
        let mount = &request.mount.digest;
        let query = match &request.mount.from {
            Some(from) => format!("mount={mount}&from={from}"),
            None => format!("mount={mount}"),
        };
        let location = append_query(
            &client::uploads_start_path(&self.url, &request.namespace),
            &query,
        );

        let (response, auth) = self
            .send_body_with_auth(&Method::POST, &location, None, Vec::new(), None)
            .await?;

        // A 201 means mounted; an advertised digest must match the request or
        // the blob would be falsely marked converged.
        if response.status() == StatusCode::CREATED {
            let advertised = advertised_digest(&response)?;
            if let Some(advertised) = advertised
                && &advertised != mount
            {
                return Err(Error::Internal(format!(
                    "mount_blob: downstream returned 201 for digest {advertised}, expected {mount}"
                )));
            }
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(write_failure("mount_blob", response.status()));
        }

        Ok(Some(UploadSession {
            url: Self::parse_location(&response)?,
            auth,
        }))
    }

    /// Streams a chunk to an upload session and returns the next continuation
    /// URL from the `Location` response header.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, access is rejected, or the
    /// response lacks a success status or `Location` header.
    #[instrument(skip(self, stream))]
    pub async fn patch_upload<S>(
        &self,
        session_url: &str,
        auth_header: Option<&str>,
        content_length: u64,
        stream: S,
    ) -> Result<String, Error>
    where
        S: AsyncRead + Unpin + Send + Sync + 'static,
    {
        let response = self
            .send_stream(
                &Method::PATCH,
                session_url,
                auth_header,
                content_length,
                stream,
            )
            .await?;

        if !response.status().is_success() {
            return Err(write_failure("patch_upload", response.status()));
        }

        Self::parse_location(&response)
    }

    /// Finalizes an upload session by committing the named `digest` (appended
    /// as the `?digest=` query parameter).
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, access is rejected, or the
    /// response is not a success status.
    #[instrument(skip(self))]
    pub async fn complete_upload(&self, session_url: &str, digest: &Digest) -> Result<(), Error> {
        let location = append_query(session_url, &format!("digest={digest}"));

        let response = self
            .send_body_with_auth(&Method::PUT, &location, None, Vec::new(), None)
            .await?
            .0;

        if !response.status().is_success() {
            return Err(write_failure("complete_upload", response.status()));
        }

        Ok(())
    }

    /// Cancels an open upload session (OCI session cancel, `DELETE` on the
    /// session URL).
    ///
    /// A `404` counts as success: an already-gone session is the goal state.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, access is rejected, or the
    /// response is a non-2xx status other than `404`.
    #[instrument(skip(self))]
    pub async fn delete_upload(&self, session_url: &str) -> Result<(), Error> {
        let response = self
            .send_body_with_auth(&Method::DELETE, session_url, None, Vec::new(), None)
            .await?
            .0;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }

        if !response.status().is_success() {
            return Err(write_failure("delete_upload", response.status()));
        }

        Ok(())
    }

    /// Classifies a replication-write `409`: `Ok(())` for a last-writer-wins
    /// rejection ([`REPLICATION_SUPERSEDED_CODE`], convergence), [`Error`] for
    /// any other 409 so the job retries or dead-letters.
    async fn classify_conflict(response: Response, op: &str) -> Result<(), Error> {
        match parse_oci_error_code(response).await.as_deref() {
            Some(REPLICATION_SUPERSEDED_CODE) => Ok(()),
            other => Err(Error::Internal(format!(
                "{op} rejected with 409 (code {})",
                other.unwrap_or("<none>")
            ))),
        }
    }

    /// Pushes a manifest by reference, stamping the `X-Angos-Source-Timestamp`
    /// header when `source_ts` is set.
    ///
    /// A missing `OCI-Subject` echo on a subject-bearing manifest signals an
    /// OCI-1.0 downstream needing the referrers fallback tag; a `409` carrying
    /// [`REPLICATION_SUPERSEDED_CODE`] sets `superseded` (convergence, not
    /// failure).
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, access is rejected, or the
    /// response is a non-2xx status other than a superseded 409.
    #[instrument(skip(self, body))]
    pub async fn put_manifest(
        &self,
        request: PutManifestRequest,
        body: Vec<u8>,
    ) -> Result<PutManifestOutcome, Error> {
        let location = client::manifest_path(&self.url, &request.namespace, &request.reference);
        let source_ts = request.source_ts.map(|ts| ts.to_rfc3339());
        let response = self
            .send_body_with_auth(
                &Method::PUT,
                &location,
                request.content_type.as_ref().map(MediaType::as_ref),
                body,
                source_ts.as_deref(),
            )
            .await?
            .0;

        if response.status() == StatusCode::CONFLICT {
            Self::classify_conflict(response, "put_manifest").await?;
            return Ok(PutManifestOutcome::Superseded);
        }

        if !response.status().is_success() {
            return Err(write_failure("put_manifest", response.status()));
        }

        // Unlike a mount, a bad echo here costs only the divergence check, so
        // it is logged rather than failing a manifest the downstream accepted.
        let digest = advertised_digest(&response).unwrap_or_else(|e| {
            warn!("put_manifest: {e}");
            None
        });
        let subject = response
            .headers()
            .get(OCI_SUBJECT)
            .and_then(|h| h.to_str().ok())
            .map(ToString::to_string);

        Ok(PutManifestOutcome::Stored { digest, subject })
    }

    /// Deletes a manifest by reference, stamping the `X-Angos-Source-Timestamp`
    /// header when `source_ts` is set.
    ///
    /// A `404` maps to [`DeleteManifestOutcome::AlreadyAbsent`], a superseded
    /// `409` to [`DeleteManifestOutcome::Superseded`], and a `405` to
    /// [`DeleteManifestOutcome::Unsupported`]; none are failures.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, access is rejected, or the
    /// response is a non-2xx status other than a 404, superseded 409, or 405.
    #[instrument(skip(self))]
    pub async fn delete_manifest(
        &self,
        request: DeleteManifestRequest,
    ) -> Result<DeleteManifestOutcome, Error> {
        let location = client::manifest_path(&self.url, &request.namespace, &request.reference);
        let source_ts = request.source_ts.map(|ts| ts.to_rfc3339());
        let response = self
            .send_body_with_auth(
                &Method::DELETE,
                &location,
                None,
                Vec::new(),
                source_ts.as_deref(),
            )
            .await?
            .0;

        if response.status() == StatusCode::CONFLICT {
            Self::classify_conflict(response, "delete_manifest").await?;
            return Ok(DeleteManifestOutcome::Superseded);
        }

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(DeleteManifestOutcome::AlreadyAbsent);
        }

        if response.status() == StatusCode::METHOD_NOT_ALLOWED {
            return Ok(DeleteManifestOutcome::Unsupported);
        }

        if !response.status().is_success() {
            return Err(write_failure("delete_manifest", response.status()));
        }

        Ok(DeleteManifestOutcome::Deleted)
    }
}
