//! Write methods for [`RegistryClient`].
//!
//! These mirror the **names** of the server-side `Registry` methods
//! (`start_upload`, `patch_upload`, `complete_upload`, `put_manifest`,
//! `delete_manifest`) but take request-side inputs (full URLs built by the
//! `upstream_url` builders, request bodies/streams) and return parsed HTTP
//! results — not the server's `HeaderMap`-bearing response structs.
//!
//! Auth, token caching, TLS and the redirect/timeout policy are reused from the
//! shared machinery on [`RegistryClient`]: byte-bodied requests go through
//! [`RegistryClient::send_body`], which mirrors [`RegistryClient::query`]
//! (cached token first, then a single refresh-and-retry on `401`). The
//! single-use upload stream in [`RegistryClient::patch_upload`] cannot be
//! replayed, so it attaches the cached token up front and surfaces a `401` as
//! an error rather than retrying with a drained body.

use reqwest::{
    Body, Method, Response, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, LINK, LOCATION},
};
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use tracing::{info, instrument};

use crate::{
    oci::Digest,
    registry::{DOCKER_CONTENT_DIGEST, Error, OCI_SUBJECT},
    registry_client::RegistryClient,
    replication::{REPLICATION_SUPERSEDED_CODE, X_ANGOS_SOURCE_TIMESTAMP},
};

/// Body of an OCI `GET /v2/<ns>/tags/list` response.
#[derive(Deserialize)]
struct TagsListBody {
    #[serde(default)]
    tags: Vec<String>,
}

/// Outcome of a manifest push.
///
/// `subject` carries the `OCI-Subject` response header when the downstream
/// processed a `subject` field itself (OCI 1.1). Its absence on a manifest that
/// declared a `subject` signals an OCI-1.0 downstream, so the pipeline must
/// push the referrers fallback tag.
///
/// `superseded` is `true` only when the downstream rejected the push with a
/// `409` whose OCI error `code` is [`REPLICATION_SUPERSEDED_CODE`] — i.e. the
/// downstream's copy is strictly newer (last-writer-wins loss). The pipeline
/// treats this as convergence (success), so `digest`/`subject` are `None` in
/// that case. Any *other* non-2xx (including a 409 with a different code, e.g.
/// an immutable-tag `CONFLICT`) is returned as [`Error`], not as `superseded`.
#[derive(Debug)]
pub struct PutManifestResult {
    pub digest: Option<Digest>,
    pub subject: Option<String>,
    pub superseded: bool,
}

/// Outcome of a manifest delete.
///
/// A `409` carrying the [`REPLICATION_SUPERSEDED_CODE`] OCI code maps to
/// [`DeleteManifestOutcome::Superseded`] (last-writer-wins loss — convergence,
/// not failure). A successful delete maps to [`DeleteManifestOutcome::Deleted`].
/// Any other non-2xx is returned as [`Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteManifestOutcome {
    /// The downstream deleted (or already lacked) the manifest.
    Deleted,
    /// The downstream rejected the delete by last-writer-wins (`409` with the
    /// replication-superseded OCI code).
    Superseded,
}

/// Reads the body of a `409` response and returns its first OCI error `code`,
/// when the body is the standard `{"errors":[{"code":"..."}]}` envelope.
///
/// Consumes the response (the body is read to completion); a malformed/absent
/// body yields `None` so the caller falls back to a generic conflict error.
async fn parse_oci_error_code(response: Response) -> Option<String> {
    let bytes = response.bytes().await.ok()?;
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()?
        .get("errors")?
        .as_array()?
        .first()?
        .get("code")?
        .as_str()
        .map(ToString::to_string)
}

impl RegistryClient {
    /// Sends a request carrying a byte body, reusing the cached auth header and
    /// refreshing once on `401` (mirrors [`RegistryClient::query`]).
    ///
    /// `source_ts` stamps the `X-Angos-Source-Timestamp` last-writer-wins header
    /// when set; pass `None` for ordinary writes.
    async fn send_body(
        &self,
        method: &Method,
        location: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
        source_ts: Option<&str>,
    ) -> Result<Response, Error> {
        info!("Writing to upstream: {method} {location}");

        let cached_auth = self.cached_auth_header(location).await;
        let response = self
            .send_body_once(
                method,
                location,
                content_type,
                body.clone(),
                cached_auth.as_deref(),
                source_ts,
            )
            .await?;

        if response.status() == StatusCode::UNAUTHORIZED {
            let token = self
                .refresh_auth_header(&response, cached_auth.as_deref())
                .await?;
            return self
                .send_body_once(
                    method,
                    location,
                    content_type,
                    body,
                    Some(&token),
                    source_ts,
                )
                .await;
        }

        if response.status() == StatusCode::FORBIDDEN {
            return Err(Error::Denied("Access forbidden".to_string()));
        }

        Ok(response)
    }

    async fn send_body_once(
        &self,
        method: &Method,
        location: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
        auth_header: Option<&str>,
        source_ts: Option<&str>,
    ) -> Result<Response, Error> {
        let mut request = self
            .build_request(method, &[], location, auth_header)
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
    /// The cached auth header is attached up front; a `401` cannot be retried
    /// because the stream has been consumed, so it is surfaced as an error.
    async fn send_stream<S>(
        &self,
        method: &Method,
        location: &str,
        content_length: u64,
        stream: S,
    ) -> Result<Response, Error>
    where
        S: AsyncRead + Unpin + Send + Sync + 'static,
    {
        info!("Streaming to upstream: {method} {location}");

        let cached_auth = self.cached_auth_header(location).await;
        let body = Body::wrap_stream(ReaderStream::new(stream));
        let response = self
            .build_request(method, &[], location, cached_auth.as_deref())
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

    /// Reads the `Location` response header and resolves it to an absolute URL.
    ///
    /// OCI registries are free to return a relative `Location` (e.g.
    /// `/v2/<ns>/blobs/uploads/<session>`); the next request needs an absolute
    /// URL, so a relative value is resolved against the response's final URL.
    fn parse_location(response: &Response) -> Result<String, Error> {
        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| {
                Error::Internal("upstream response is missing Location header".into())
            })?;

        response
            .url()
            .join(location)
            .map(|url| url.to_string())
            .map_err(|e| Error::Internal(format!("invalid Location header '{location}': {e}")))
    }

    /// Starts a resumable blob upload session, returning its continuation URL.
    ///
    /// The session continuation URLs (PATCH/PUT targets) come from the `Location`
    /// response header — they are server-assigned, never synthesized.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects access, or the
    /// response omits the `Location` header / is not a success status.
    #[instrument(skip(self))]
    pub async fn start_upload(&self, location: &str) -> Result<String, Error> {
        let response = self
            .send_body(&Method::POST, location, None, Vec::new(), None)
            .await?;

        if !response.status().is_success() {
            return Err(Error::Internal(format!(
                "start_upload failed with status {}",
                response.status()
            )));
        }

        Self::parse_location(&response)
    }

    /// Attempts an OCI cross-repository blob mount.
    ///
    /// Appends `?mount=<digest>[&from=<repo>]` so the server can grant a reference
    /// without a body transfer. The return distinguishes the two outcomes:
    /// - `Ok(None)` — the mount was satisfied (`201 Created`); no transfer
    ///   needed. When the `201` advertises a `Docker-Content-Digest` it is
    ///   verified against the requested digest (a mismatch is an error).
    /// - `Ok(Some(session_url))` — the mount could not be satisfied (`202
    ///   Accepted`); a session was opened and the caller uploads the bytes.
    ///
    /// Per the distribution spec a mount that cannot be satisfied degrades to a
    /// normal upload rather than failing.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects the mount, or a
    /// session response omits the `Location` header / is not a success status.
    #[instrument(skip(self))]
    pub async fn mount_blob(
        &self,
        location: &str,
        mount: &Digest,
        from: Option<&str>,
    ) -> Result<Option<String>, Error> {
        let separator = if location.contains('?') { '&' } else { '?' };
        let location = match from {
            Some(from) => format!("{location}{separator}mount={mount}&from={from}"),
            None => format!("{location}{separator}mount={mount}"),
        };

        let response = self
            .send_body(&Method::POST, &location, None, Vec::new(), None)
            .await?;

        // 201 Created: the blob is already present downstream (mounted). The
        // digest header is optional, but if present it must name the requested
        // blob — a different digest would falsely mark the blob converged.
        if response.status() == StatusCode::CREATED {
            let advertised = response
                .headers()
                .get(DOCKER_CONTENT_DIGEST)
                .and_then(|h| h.to_str().ok())
                .and_then(|s| Digest::try_from(s).ok());
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
            return Err(Error::Internal(format!(
                "mount_blob failed with status {}",
                response.status()
            )));
        }

        Self::parse_location(&response).map(Some)
    }

    /// Streams a chunk to an in-progress upload session and returns the next
    /// continuation URL from the `Location` response header.
    ///
    /// `session_url` is the URL returned by [`RegistryClient::start_upload`] (or
    /// a previous `patch_upload`).
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects access, or
    /// the response is not a success status / omits the `Location` header.
    #[instrument(skip(self, stream))]
    pub async fn patch_upload<S>(
        &self,
        session_url: &str,
        content_length: u64,
        stream: S,
    ) -> Result<String, Error>
    where
        S: AsyncRead + Unpin + Send + Sync + 'static,
    {
        let response = self
            .send_stream(&Method::PATCH, session_url, content_length, stream)
            .await?;

        if !response.status().is_success() {
            return Err(Error::Internal(format!(
                "patch_upload failed with status {}",
                response.status()
            )));
        }

        Self::parse_location(&response)
    }

    /// Finalizes an upload session by committing the named `digest`.
    ///
    /// `session_url` is the continuation URL from the last
    /// [`RegistryClient::patch_upload`] (or [`RegistryClient::start_upload`] for
    /// a monolithic upload). The `?digest=` query parameter is appended here.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects access, or
    /// the response is not a success status.
    #[instrument(skip(self))]
    pub async fn complete_upload(&self, session_url: &str, digest: &Digest) -> Result<(), Error> {
        let separator = if session_url.contains('?') { '&' } else { '?' };
        let location = format!("{session_url}{separator}digest={digest}");

        let response = self
            .send_body(&Method::PUT, &location, None, Vec::new(), None)
            .await?;

        if !response.status().is_success() {
            return Err(Error::Internal(format!(
                "complete_upload failed with status {}",
                response.status()
            )));
        }

        Ok(())
    }

    /// Classifies a `409 Conflict` from a replication write. `Ok(())` means the
    /// downstream rejected by last-writer-wins (OCI code
    /// [`REPLICATION_SUPERSEDED_CODE`] — its copy is strictly newer), which the
    /// caller maps to its own convergence outcome. Any other 409 (e.g. an
    /// immutable-tag `CONFLICT`) returns [`Error`] so the job retries/dead-letters.
    ///
    /// # Errors
    ///
    /// Returns an error for any non-superseded 409.
    async fn classify_conflict(response: Response, op: &str) -> Result<(), Error> {
        match parse_oci_error_code(response).await.as_deref() {
            Some(REPLICATION_SUPERSEDED_CODE) => Ok(()),
            other => Err(Error::Internal(format!(
                "{op} rejected with 409 (code {})",
                other.unwrap_or("<none>")
            ))),
        }
    }

    /// Pushes a manifest by reference.
    ///
    /// `source_ts` stamps the `X-Angos-Source-Timestamp` header when set (a
    /// replication push); it is `None` for an ordinary push.
    ///
    /// Surfaces the `OCI-Subject` response header so the pipeline can detect an
    /// OCI-1.0 downstream (no header on a subject-bearing manifest) and push the
    /// referrers fallback tag.
    ///
    /// 409 disambiguation: a `409` whose OCI error `code` is
    /// [`REPLICATION_SUPERSEDED_CODE`] (the downstream's copy is strictly newer —
    /// a last-writer-wins loss) returns
    /// `PutManifestResult { superseded: true, .. }` so the pipeline can treat it
    /// as convergence (success). Any *other* 409 (e.g. an immutable-tag
    /// `CONFLICT`) and every other non-2xx is returned as [`Error`] so the job
    /// retries/dead-letters.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects access, or
    /// the response is a non-2xx status other than an LWW-superseded 409.
    #[instrument(skip(self, body))]
    pub async fn put_manifest(
        &self,
        location: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
        source_ts: Option<&str>,
    ) -> Result<PutManifestResult, Error> {
        let response = self
            .send_body(&Method::PUT, location, content_type, body, source_ts)
            .await?;

        if response.status() == StatusCode::CONFLICT {
            Self::classify_conflict(response, "put_manifest").await?;
            return Ok(PutManifestResult {
                digest: None,
                subject: None,
                superseded: true,
            });
        }

        if !response.status().is_success() {
            return Err(Error::Internal(format!(
                "put_manifest failed with status {}",
                response.status()
            )));
        }

        let digest = response
            .headers()
            .get(DOCKER_CONTENT_DIGEST)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| Digest::try_from(s).ok());
        let subject = response
            .headers()
            .get(OCI_SUBJECT)
            .and_then(|h| h.to_str().ok())
            .map(ToString::to_string);

        Ok(PutManifestResult {
            digest,
            subject,
            superseded: false,
        })
    }

    /// Deletes a manifest by reference.
    ///
    /// `source_ts` stamps the `X-Angos-Source-Timestamp` header when set (a
    /// replication delete); it is `None` for an ordinary delete.
    ///
    /// 409 disambiguation: a `409` whose OCI error `code` is
    /// [`REPLICATION_SUPERSEDED_CODE`] returns
    /// [`DeleteManifestOutcome::Superseded`] (a last-writer-wins loss —
    /// convergence). Any *other* 409 and every other non-2xx is returned as
    /// [`Error`].
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects access, or
    /// the response is a non-2xx status other than an LWW-superseded 409.
    #[instrument(skip(self))]
    pub async fn delete_manifest(
        &self,
        location: &str,
        source_ts: Option<&str>,
    ) -> Result<DeleteManifestOutcome, Error> {
        let response = self
            .send_body(&Method::DELETE, location, None, Vec::new(), source_ts)
            .await?;

        if response.status() == StatusCode::CONFLICT {
            Self::classify_conflict(response, "delete_manifest").await?;
            return Ok(DeleteManifestOutcome::Superseded);
        }

        if !response.status().is_success() {
            return Err(Error::Internal(format!(
                "delete_manifest failed with status {}",
                response.status()
            )));
        }

        Ok(DeleteManifestOutcome::Deleted)
    }

    /// Lists every tag of a repository on the downstream, following pagination.
    ///
    /// `location` is the absolute `GET /v2/<ns>/tags/list` URL (from
    /// [`RegistryClient::get_tags_list_path`]). The `{"tags":[...]}` bodies are
    /// concatenated across pages; pagination follows the `Link` rel="next" header
    /// (the spec's `?n=&last=` continuation). A `404` means the repository is
    /// absent on the downstream, which yields an empty list rather than an error.
    ///
    /// Auth/token handling is reused from the shared GET machinery
    /// ([`RegistryClient::query`]), exactly like `head_manifest` / `get_manifest`.
    ///
    /// # Errors
    ///
    /// Returns an error when a request fails, the downstream rejects access, or a
    /// (non-404) page is a non-success status / has an unparseable body.
    #[instrument(skip(self))]
    pub async fn list_tags(&self, location: &str) -> Result<Vec<String>, Error> {
        let mut tags = Vec::new();
        let mut next = Some(location.to_string());

        while let Some(page_location) = next.take() {
            let response = self.query(&Method::GET, &[], &page_location).await?;

            if response.status() == StatusCode::NOT_FOUND {
                // Repository absent on the downstream: nothing to reconcile.
                return Ok(tags);
            }
            if !response.status().is_success() {
                return Err(Error::Internal(format!(
                    "list_tags failed with status {}",
                    response.status()
                )));
            }

            next = Self::parse_next_link(&response);

            let body = response
                .bytes()
                .await
                .map_err(|e| Error::Internal(format!("failed to read tags/list body: {e}")))?;
            let parsed: TagsListBody = serde_json::from_slice(&body)
                .map_err(|e| Error::Internal(format!("failed to parse tags/list body: {e}")))?;
            tags.extend(parsed.tags);
        }

        Ok(tags)
    }

    /// Resolves the `Link` rel="next" continuation URL from a `tags/list`
    /// response against the response's final URL, or `None` when absent.
    ///
    /// The header value has the form `<...>; rel="next"`; only the bracketed URL
    /// of a `rel="next"` entry is followed.
    fn parse_next_link(response: &Response) -> Option<String> {
        let header = response.headers().get(LINK)?.to_str().ok()?;
        let url = header
            .split(',')
            .filter(|entry| entry.contains("rel=\"next\"") || entry.contains("rel=next"))
            .find_map(|entry| {
                let start = entry.find('<')?;
                let end = entry[start + 1..].find('>')? + start + 1;
                Some(&entry[start + 1..end])
            })?;
        response.url().join(url).ok().map(|u| u.to_string())
    }
}
