//! The serving side of the protocol: reading what a client asked for, and
//! rendering the header vocabulary of the answer. The request itself is
//! [`crate::request`], which both ends share.

use std::cmp::Reverse;

use http::{
    HeaderMap,
    header::{
        ACCEPT, ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, HeaderValue,
        InvalidHeaderValue, LINK, LOCATION, RANGE,
    },
};

use crate::client::{blob_path, manifest_path, uploads_start_path};
use crate::header::{
    APPLICATION_JSON, DOCKER_CONTENT_DIGEST, DOCKER_DISTRIBUTION_API_VERSION,
    DOCKER_DISTRIBUTION_API_VERSION_V2, DOCKER_UPLOAD_UUID, LINK_REL_NEXT, OCI_FILTERS_APPLIED,
    OCI_INDEX_CONTENT_TYPE, OCI_SUBJECT, OCI_TAG,
};
use crate::path::{BLOBS, MANIFESTS, REFERRERS, UPLOADS};
use crate::types::http_range::ResponseRange;
use crate::types::{Digest, MediaRange, MediaType, Namespace, Reference, Tag, UploadSessionId};

static QUALITY_PARAM: &str = "q";

/// One `Accept` member: its media range and the quality it was offered at.
struct AcceptMediaRange {
    quality: u16,
    value: MediaRange,
}

impl AcceptMediaRange {
    /// Reads one comma-separated member of an `Accept` header. `None` when it
    /// is not a media range, which is dropped rather than forwarded: angos
    /// re-sends these upstream and must not relay a malformed `Accept`.
    fn new(member: &str) -> Option<Self> {
        let value = MediaRange::new(member.trim()).ok()?;
        let quality = quality(value.as_str());
        Some(Self { quality, value })
    }
}

/// The weight, out of 1000, a header member names in its `q` parameter. Without
/// one it is offered at full weight; one angos cannot read is not a weight it
/// may invent, so it weighs nothing.
#[must_use]
pub fn quality(member: &str) -> u16 {
    member
        .split(';')
        .skip(1)
        .filter_map(|parameter| parameter.trim().split_once('='))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case(QUALITY_PARAM))
        .map_or(1000, |(_, quality)| {
            parse_quality(quality.trim()).unwrap_or(0)
        })
}

/// The media ranges an `Accept` header offers, most preferred first.
#[must_use]
pub fn accepted_content_types(headers: &HeaderMap) -> Vec<MediaRange> {
    let mut media_ranges: Vec<AcceptMediaRange> = headers
        .get_all(ACCEPT)
        .iter()
        .filter_map(|header| header.to_str().ok())
        .flat_map(|header| header.split(','))
        .filter_map(AcceptMediaRange::new)
        .collect();

    // A stable sort, so members offered at the same quality keep the order the
    // client sent them in.
    media_ranges.sort_by_key(|media_range| Reverse(media_range.quality));

    media_ranges
        .into_iter()
        .map(|media_range| media_range.value)
        .collect()
}

fn parse_quality(value: &str) -> Option<u16> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));

    match whole {
        "1" if fraction.chars().all(|digit| digit == '0') && fraction.len() <= 3 => Some(1000),
        "0" if fraction.chars().all(|digit| digit.is_ascii_digit()) && fraction.len() <= 3 => {
            let mut quality = 0;
            for index in 0..3 {
                quality *= 10;
                if let Some(digit) = fraction.as_bytes().get(index) {
                    quality += u16::from(*digit - b'0');
                }
            }

            Some(quality)
        }
        _ => None,
    }
}

/// Headers of the version handshake: the API this registry speaks, which the
/// protocol pins rather than the deployment choosing.
#[must_use]
pub fn api_version_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        DOCKER_DISTRIBUTION_API_VERSION,
        DOCKER_DISTRIBUTION_API_VERSION_V2,
    );

    headers
}

/// Headers of a paginated JSON listing: its content type, and the `Link` to the
/// next page while the listing is not exhausted.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn paginated_json_headers(next_page: Option<&str>) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, APPLICATION_JSON);
    if let Some(next_page) = next_page {
        headers.insert(LINK, next_page_link(next_page)?);
    }

    Ok(headers)
}

/// Headers of a referrers listing: the index media type, the `Link` to the next
/// page when there is one, and the filter the listing honoured, which lets a
/// client tell a filtered index from a complete one.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn referrers_headers(
    artifact_type_filtered: bool,
    next_page: Option<&str>,
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, OCI_INDEX_CONTENT_TYPE);
    if artifact_type_filtered {
        headers.insert(
            OCI_FILTERS_APPLIED,
            HeaderValue::from_static("artifactType"),
        );
    }
    if let Some(next_page) = next_page {
        headers.insert(LINK, next_page_link(next_page)?);
    }

    Ok(headers)
}

/// Renders `target` as the `rel="next"` entry of a `Link` header, which a
/// paginated listing advertises its next page with.
///
/// # Errors
///
/// Returns an error when `target` holds bytes no header value may carry.
pub fn next_page_link(target: &str) -> Result<HeaderValue, InvalidHeaderValue> {
    HeaderValue::try_from(format!("<{target}>; {LINK_REL_NEXT}"))
}

/// Splits `<namespace>/manifests/<reference>` out of a request path, which is
/// what [`crate::client::manifest_path`] builds. The path is the one after the
/// API prefix; the pieces come back unvalidated, for the caller to parse.
#[must_use]
pub fn split_manifest_path(api_path: &str) -> Option<(&str, &str)> {
    api_path.rsplit_once(MANIFESTS)
}

/// Splits `<namespace>/blobs/<digest>`, the counterpart of
/// [`crate::client::blob_path`].
#[must_use]
pub fn split_blob_path(api_path: &str) -> Option<(&str, &str)> {
    api_path.rsplit_once(BLOBS)
}

/// Splits `<namespace>/referrers/<digest>`, the counterpart of
/// [`crate::client::referrers_path`].
#[must_use]
pub fn split_referrers_path(api_path: &str) -> Option<(&str, &str)> {
    api_path.rsplit_once(REFERRERS)
}

/// The namespace of `<namespace>/blobs/uploads/`, the counterpart of
/// [`crate::client::uploads_start_path`]. The trailing slash is part of the
/// endpoint the spec defines, so a path without it is not this endpoint.
#[must_use]
pub fn split_uploads_start_path(api_path: &str) -> Option<&str> {
    api_path.strip_suffix(UPLOADS)
}

/// Headers a blob answer carries whether or not it has a body: `Accept-Ranges`
/// among them, so a `HEAD` advertises the ranges a `GET` will serve.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn blob_headers(digest: &Digest, size: u64) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    headers.insert(DOCKER_CONTENT_DIGEST, HeaderValue::try_from(digest)?);
    headers.insert(CONTENT_LENGTH, size.into());
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    Ok(headers)
}

/// Headers for a `206`. `length` is the body actually streamed, which an
/// upstream reports separately from the window it says it served.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn partial_blob_headers(
    digest: &Digest,
    length: u64,
    range: ResponseRange,
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = blob_headers(digest, length)?;
    headers.insert(CONTENT_RANGE, HeaderValue::try_from(range)?);

    Ok(headers)
}

/// Headers redirecting a blob read to storage.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn blob_redirect_headers(url: &str, digest: &Digest) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    headers.insert(LOCATION, HeaderValue::try_from(url)?);
    headers.insert(DOCKER_CONTENT_DIGEST, HeaderValue::try_from(digest)?);

    Ok(headers)
}

/// Headers naming a stored blob: the answer to a completed upload, and to one
/// whose digest the registry already held.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn blob_location_headers(
    namespace: &Namespace,
    digest: &Digest,
) -> Result<HeaderMap, InvalidHeaderValue> {
    // The same path grammar the requesting side builds, rooted at the registry.
    blob_redirect_headers(&blob_path("", namespace, digest), digest)
}

/// Headers naming an open upload session, which the client continues against.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn session_location_headers(
    namespace: &Namespace,
    session_id: &UploadSessionId,
) -> Result<HeaderMap, InvalidHeaderValue> {
    // The session continues under the endpoint that opened it, rooted at the
    // registry, which is the same grammar the requesting side builds.
    let location = format!("{}{session_id}", uploads_start_path("", namespace));
    let mut headers = HeaderMap::new();
    headers.insert(LOCATION, HeaderValue::try_from(location)?);
    headers.insert(
        DOCKER_UPLOAD_UUID,
        HeaderValue::try_from(session_id.to_string())?,
    );

    Ok(headers)
}

/// [`session_location_headers`] for a session that has taken no bytes yet.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn upload_session_headers(
    namespace: &Namespace,
    session_id: &UploadSessionId,
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = session_location_headers(namespace, session_id)?;
    headers.insert(RANGE, HeaderValue::from_static("0-0"));

    Ok(headers)
}

/// [`session_location_headers`] plus the bytes committed so far, so a resuming
/// client knows where to continue from.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn upload_progress_headers(
    namespace: &Namespace,
    session_id: &UploadSessionId,
    size: u64,
    content_length: Option<u64>,
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = session_location_headers(namespace, session_id)?;
    headers.insert(
        RANGE,
        HeaderValue::try_from(format!("0-{}", size.saturating_sub(1)))?,
    );
    if let Some(length) = content_length {
        headers.insert(CONTENT_LENGTH, length.into());
    }

    Ok(headers)
}

/// `Docker-Content-Digest` and `Content-Length` for a served manifest, plus its
/// `Content-Type` when one is recorded.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn manifest_headers(
    media_type: Option<&MediaType>,
    digest: &Digest,
    size: u64,
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    headers.insert(DOCKER_CONTENT_DIGEST, HeaderValue::try_from(digest)?);
    headers.insert(CONTENT_LENGTH, size.into());
    if let Some(media_type) = media_type {
        headers.insert(CONTENT_TYPE, HeaderValue::try_from(media_type.to_string())?);
    }

    Ok(headers)
}

/// Headers redirecting a manifest read to storage.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn manifest_redirect_headers(
    url: &str,
    digest: &Digest,
    media_type: Option<&MediaType>,
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = blob_redirect_headers(url, digest)?;
    if let Some(media_type) = media_type {
        headers.insert(CONTENT_TYPE, HeaderValue::try_from(media_type.to_string())?);
    }

    Ok(headers)
}

/// Headers answering a manifest push: where it landed, the digest it stored
/// under, the subject it was indexed against, and the tags the push bound.
///
/// # Errors
///
/// Returns an error when a value cannot be carried by a header.
pub fn put_manifest_headers(
    namespace: &Namespace,
    reference: &Reference,
    digest: &Digest,
    subject: Option<&Digest>,
    created_tags: &[Tag],
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    headers.insert(
        LOCATION,
        HeaderValue::try_from(manifest_path("", namespace, reference))?,
    );
    headers.insert(DOCKER_CONTENT_DIGEST, HeaderValue::try_from(digest)?);
    if let Some(subject) = subject {
        headers.insert(OCI_SUBJECT, HeaderValue::try_from(subject)?);
    }
    if !created_tags.is_empty() {
        let joined = created_tags
            .iter()
            .map(Tag::as_ref)
            .collect::<Vec<&str>>()
            .join(", ");
        headers.insert(OCI_TAG, HeaderValue::try_from(joined)?);
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use crate::client::{blob_path, manifest_path, uploads_start_path};
    use crate::path::API_PREFIX;
    use crate::server::*;
    use crate::types::Reference;

    fn accept(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(ACCEPT, HeaderValue::from_str(value).unwrap());
        }

        headers
    }

    fn ranges(headers: &HeaderMap) -> Vec<String> {
        accepted_content_types(headers)
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn a_member_that_is_not_a_media_range_is_dropped() {
        assert_eq!(
            ranges(&accept(&["application/json, not-a-range"])),
            vec!["application/json"]
        );
    }

    #[test]
    fn no_accept_header_offers_nothing() {
        assert!(ranges(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn members_are_ordered_by_descending_quality() {
        assert_eq!(
            ranges(&accept(&["a/b;q=0.2, c/d, e/f;q=0.5"])),
            vec!["c/d", "e/f;q=0.5", "a/b;q=0.2"]
        );
    }

    #[test]
    fn equal_quality_keeps_the_order_the_client_sent() {
        assert_eq!(
            ranges(&accept(&["a/b;q=0.5", "c/d;q=0.5"])),
            vec!["a/b;q=0.5", "c/d;q=0.5"]
        );
    }

    fn quality_of(member: &str) -> u16 {
        AcceptMediaRange::new(member)
            .expect("the fixture must be a media range")
            .quality
    }

    // A `q` angos cannot read is not a preference it may invent: the member
    // sorts last rather than being promoted to full weight.
    #[test]
    fn an_unreadable_quality_sorts_last() {
        for value in ["q=2", "q=0.1234", "q=abc", "q="] {
            assert_eq!(quality_of(&format!("a/b;{value}")), 0, "{value}");
        }
        assert_eq!(quality_of("a/b"), 1000);
        assert_eq!(quality_of("a/b;q=1.0"), 1000);
        assert_eq!(quality_of("a/b;q=0.75"), 750);
    }

    const SAMPLE_SESSION: &str = "067e6162-3b6f-4ae2-a171-2470b63dff00";
    const MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

    fn digest() -> Digest {
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .parse()
            .unwrap()
    }

    fn namespace() -> Namespace {
        Namespace::new("test-repo").unwrap()
    }

    fn session_id() -> UploadSessionId {
        UploadSessionId::new(SAMPLE_SESSION).unwrap()
    }

    /// `Accept-Ranges` rides on every blob answer, `HEAD` included: a client
    /// must be able to learn a blob is rangeable without fetching it.
    #[test]
    fn blob_headers_advertise_ranges() {
        let headers = blob_headers(&digest(), 42).unwrap();
        assert_eq!(headers[&DOCKER_CONTENT_DIGEST], digest().to_string());
        assert_eq!(headers[&CONTENT_LENGTH], "42");
        assert_eq!(headers[&ACCEPT_RANGES], "bytes");
    }

    /// `Content-Length` is the streamed body, which an upstream reports apart
    /// from the window its `Content-Range` names.
    #[test]
    fn partial_blob_headers_carry_length_and_content_range() {
        let range = ResponseRange {
            start: 5,
            end: 10,
            total_length: Some(100),
        };
        let headers = partial_blob_headers(&digest(), range.length(), range).unwrap();
        assert_eq!(headers[&CONTENT_LENGTH], "6");
        assert_eq!(headers[&CONTENT_RANGE], "bytes 5-10/100");
    }

    #[test]
    fn blob_redirect_headers_carry_location_and_digest() {
        let headers = blob_redirect_headers("https://cdn/blob", &digest()).unwrap();
        assert_eq!(headers[&LOCATION], "https://cdn/blob");
        assert_eq!(headers[&DOCKER_CONTENT_DIGEST], digest().to_string());
    }

    #[test]
    fn blob_location_headers_carry_location_and_digest() {
        let (namespace, digest) = (namespace(), digest());
        let headers = blob_location_headers(&namespace, &digest).unwrap();
        assert_eq!(
            headers[&LOCATION],
            format!("/v2/{namespace}/blobs/{digest}")
        );
        assert_eq!(headers[&DOCKER_CONTENT_DIGEST], digest.to_string());
    }

    #[test]
    fn upload_session_headers_start_at_zero_range() {
        let namespace = namespace();
        let headers = upload_session_headers(&namespace, &session_id()).unwrap();
        assert_eq!(
            headers[&LOCATION],
            format!("/v2/{namespace}/blobs/uploads/{SAMPLE_SESSION}")
        );
        assert_eq!(headers[&RANGE], "0-0");
        assert_eq!(headers[&DOCKER_UPLOAD_UUID], SAMPLE_SESSION);
    }

    /// `Range` reports the bytes committed so far. A `PATCH` answers with an
    /// empty body and declares `Content-Length: 0`; a `GET` declares none.
    #[test]
    fn upload_progress_headers_report_current_range() {
        let headers = upload_progress_headers(&namespace(), &session_id(), 42, None).unwrap();
        assert_eq!(headers[&RANGE], "0-41");
        assert!(!headers.contains_key(CONTENT_LENGTH));

        let headers = upload_progress_headers(&namespace(), &session_id(), 42, Some(0)).unwrap();
        assert_eq!(headers[&RANGE], "0-41");
        assert_eq!(headers[&CONTENT_LENGTH], "0");
    }

    #[test]
    fn manifest_headers_carry_digest_length_and_media_type() {
        let media_type = MediaType::new(MANIFEST_MEDIA_TYPE).unwrap();
        let headers = manifest_headers(Some(&media_type), &digest(), 42).unwrap();
        assert_eq!(headers[&DOCKER_CONTENT_DIGEST], digest().to_string());
        assert_eq!(headers[&CONTENT_LENGTH], "42");
        assert_eq!(headers[&CONTENT_TYPE], MANIFEST_MEDIA_TYPE);

        let headers = manifest_headers(None, &digest(), 7).unwrap();
        assert!(!headers.contains_key(CONTENT_TYPE));
    }

    #[test]
    fn manifest_redirect_headers_carry_location_digest_and_media_type() {
        let media_type = MediaType::new(MANIFEST_MEDIA_TYPE).unwrap();
        let headers =
            manifest_redirect_headers("https://cdn/manifest", &digest(), Some(&media_type))
                .unwrap();
        assert_eq!(headers[&LOCATION], "https://cdn/manifest");
        assert_eq!(headers[&DOCKER_CONTENT_DIGEST], digest().to_string());
        assert_eq!(headers[&CONTENT_TYPE], MANIFEST_MEDIA_TYPE);
    }

    #[test]
    fn put_manifest_headers_name_where_the_manifest_landed() {
        let namespace = Namespace::new("test/repo").unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let headers = put_manifest_headers(&namespace, &reference, &digest(), None, &[]).unwrap();
        assert_eq!(headers[&LOCATION], "/v2/test/repo/manifests/latest");
        assert_eq!(headers[&DOCKER_CONTENT_DIGEST], digest().to_string());
        assert!(!headers.contains_key(&OCI_SUBJECT));
        assert!(!headers.contains_key(&OCI_TAG));
    }

    #[test]
    fn put_manifest_headers_echo_the_subject_and_the_tags_bound() {
        let namespace = Namespace::new("test/repo").unwrap();
        let reference = Reference::Digest(digest());
        let tags = vec![Tag::new("1.2.3").unwrap(), Tag::new("latest").unwrap()];
        let headers =
            put_manifest_headers(&namespace, &reference, &digest(), Some(&digest()), &tags)
                .unwrap();
        assert_eq!(headers[&OCI_SUBJECT], digest().to_string());
        assert_eq!(headers[&OCI_TAG], "1.2.3, latest");
    }

    // What one end builds is what the other takes apart, so the grammar is
    // pinned across the pair rather than in either half alone.
    #[test]
    fn a_built_url_splits_back_into_its_pieces() {
        let namespace = Namespace::new("test/repo").unwrap();
        let reference = Reference::Tag(Tag::new("latest").unwrap());
        let digest = digest();
        let after_prefix = |url: String| url.strip_prefix(API_PREFIX).unwrap().to_string();

        let manifest = after_prefix(manifest_path("", &namespace, &reference));
        assert_eq!(
            split_manifest_path(&manifest),
            Some(("test/repo", "latest"))
        );

        let blob = after_prefix(blob_path("", &namespace, &digest));
        assert_eq!(
            split_blob_path(&blob),
            Some(("test/repo", digest.to_string().as_str()))
        );

        let uploads = after_prefix(uploads_start_path("", &namespace));
        assert_eq!(split_uploads_start_path(&uploads), Some("test/repo"));
    }

    /// The spec spells the endpoint with its trailing slash (end-4a), so a path
    /// without one is a different path and must not route here.
    #[test]
    fn the_uploads_endpoint_needs_its_trailing_slash() {
        assert_eq!(
            split_uploads_start_path("repo/blobs/uploads/"),
            Some("repo")
        );
        assert_eq!(split_uploads_start_path("repo/blobs/uploads"), None);
        assert_eq!(split_uploads_start_path("repo/blobs/other"), None);
    }

    #[test]
    fn api_version_headers_name_the_protocol() {
        let headers = api_version_headers();
        assert_eq!(headers[&DOCKER_DISTRIBUTION_API_VERSION], "registry/2.0");
    }

    #[test]
    fn paginated_json_headers_advertise_the_next_page_only_when_there_is_one() {
        let headers = paginated_json_headers(Some("/v2/nginx/tags/list?n=1")).unwrap();
        assert_eq!(headers[&CONTENT_TYPE], "application/json");
        assert_eq!(headers[&LINK], "</v2/nginx/tags/list?n=1>; rel=\"next\"");

        let headers = paginated_json_headers(None).unwrap();
        assert_eq!(headers[&CONTENT_TYPE], "application/json");
        assert!(!headers.contains_key(LINK));
    }

    #[test]
    fn next_page_link_renders_the_rel_next_entry() {
        let header = next_page_link("/v2/nginx/tags/list?n=1").unwrap();
        assert_eq!(header, "</v2/nginx/tags/list?n=1>; rel=\"next\"");
    }
}
