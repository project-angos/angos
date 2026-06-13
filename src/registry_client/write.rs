//! Write methods for [`RegistryClient`].
//!
//! Byte-bodied requests reuse [`RegistryClient::send_with_auth_retry`]'s
//! cached-token-then-single-refresh orchestration; the single-use stream in
//! [`RegistryClient::patch_upload`] cannot be replayed, so it surfaces a `401`
//! as an error instead of retrying.

use std::collections::HashSet;

use bytes::Bytes;
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
/// A missing `subject` echo on a subject-bearing manifest signals an OCI-1.0
/// downstream needing the referrers fallback tag; `superseded` is `true` only
/// for a `409` with [`REPLICATION_SUPERSEDED_CODE`], which is convergence, not
/// failure.
#[derive(Debug)]
pub struct PutManifestResult {
    pub digest: Option<Digest>,
    pub subject: Option<String>,
    pub superseded: bool,
}

/// Outcome of a manifest delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteManifestOutcome {
    /// Downstream applied the delete (2xx).
    Deleted,
    /// Manifest already absent downstream (`404`): a converged no-op.
    AlreadyAbsent,
    /// Delete rejected by last-writer-wins (`409` with the
    /// replication-superseded OCI code): convergence, not failure.
    Superseded,
    /// Downstream rejects this delete method (`405`): it does not support
    /// deleting by this reference (stock `distribution` rejects tag deletes
    /// this way). Retrying cannot help, so the caller records it distinctly
    /// instead of dead-lettering one job per deletion event.
    Unsupported,
}

/// Returns the first OCI error `code` from a `{"errors":[{"code":"..."}]}`
/// response body, or `None` when the body is malformed or absent.
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

/// Appends a query string to a URL, choosing '?' or '&' for the separator.
fn append_query(base: &str, query: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}{query}")
}

impl RegistryClient {
    /// Sends a byte-bodied request via [`RegistryClient::send_with_auth_retry`].
    ///
    /// `source_ts`, when set, stamps the `X-Angos-Source-Timestamp`
    /// last-writer-wins header.
    async fn send_body(
        &self,
        method: &Method,
        location: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
        source_ts: Option<&str>,
    ) -> Result<Response, Error> {
        info!("Writing to upstream: {method} {location}");

        // `Bytes` makes the per-attempt clone a refcount bump, not a deep copy.
        let body = Bytes::from(body);

        self.send_with_auth_retry(location, |auth| {
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
    /// The consumed stream cannot be replayed, so a `401` is surfaced as an
    /// error instead of a token-refresh retry.
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

        response
            .url()
            .join(location)
            .map(|url| url.to_string())
            .map_err(|e| Error::Internal(format!("invalid Location header '{location}': {e}")))
    }

    /// Starts a resumable blob upload session, returning the server-assigned
    /// continuation URL from the `Location` header.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, access is rejected, or the
    /// response lacks a success status or `Location` header.
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

    /// Attempts an OCI cross-repository blob mount
    /// (`?mount=<digest>[&from=<repo>]`).
    ///
    /// `Ok(None)` means mounted (`201`, with the advertised digest verified
    /// when present); `Ok(Some(session_url))` means the mount degraded to a
    /// normal upload session (`202`), per the distribution spec.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails, the server rejects the mount,
    /// or a session response lacks a success status or `Location` header.
    #[instrument(skip(self))]
    pub async fn mount_blob(
        &self,
        location: &str,
        mount: &Digest,
        from: Option<&str>,
    ) -> Result<Option<String>, Error> {
        let query = match from {
            Some(from) => format!("mount={mount}&from={from}"),
            None => format!("mount={mount}"),
        };
        let location = append_query(location, &query);

        let response = self
            .send_body(&Method::POST, &location, None, Vec::new(), None)
            .await?;

        // A 201 means mounted; an advertised digest must match the request or
        // the blob would be falsely marked converged.
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
            .send_body(&Method::DELETE, session_url, None, Vec::new(), None)
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }

        if !response.status().is_success() {
            return Err(Error::Internal(format!(
                "delete_upload failed with status {}",
                response.status()
            )));
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

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(DeleteManifestOutcome::AlreadyAbsent);
        }

        if response.status() == StatusCode::METHOD_NOT_ALLOWED {
            return Ok(DeleteManifestOutcome::Unsupported);
        }

        if !response.status().is_success() {
            return Err(Error::Internal(format!(
                "delete_manifest failed with status {}",
                response.status()
            )));
        }

        Ok(DeleteManifestOutcome::Deleted)
    }

    /// Lists every tag of a repository on the downstream, following `Link`
    /// rel="next" pagination.
    ///
    /// A `404` (repository absent downstream) yields an empty list rather than
    /// an error.
    ///
    /// # Errors
    ///
    /// Returns an error when a request fails, access is rejected, or a non-404
    /// page has a non-success status or unparseable body.
    #[instrument(skip(self))]
    pub async fn list_tags(&self, location: &str) -> Result<Vec<String>, Error> {
        // Guards against non-terminating pagination: `visited` stops an exact
        // page-URL cycle; `MAX_PAGES` backstops endless distinct pages.
        const MAX_PAGES: usize = 10_000;

        let mut tags = Vec::new();
        let mut next = Some(location.to_string());
        let mut visited = HashSet::new();

        while let Some(page_location) = next.take() {
            if visited.len() >= MAX_PAGES {
                return Err(Error::Internal(format!(
                    "list_tags exceeded {MAX_PAGES} pages; downstream pagination does not terminate"
                )));
            }
            if !visited.insert(page_location.clone()) {
                // Cyclic `rel="next"`: stop with the tags gathered so far (a
                // partial list only under-reconciles).
                break;
            }

            let response = self.query(&Method::GET, &[], &page_location).await?;

            if response.status() == StatusCode::NOT_FOUND {
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

    /// Extracts the `Link` rel="next" continuation URL (`<...>; rel="next"`),
    /// resolved against the response's final URL, or `None` when absent.
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
