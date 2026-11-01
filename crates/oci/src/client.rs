//! The requesting side of the protocol: turning a request into the URL that
//! carries it, and reading the header vocabulary of the answer back. The
//! request itself is [`crate::request`], which both ends share.

use crate::path::{API_PREFIX, BLOBS, MANIFESTS, REFERRERS, TAGS_LIST, UPLOADS};
use crate::request::{GetReferrersRequest, ListTagsRequest};
use crate::types::{Digest, Namespace, Reference};

/// URL of a manifest by reference (`/v2/<ns>/manifests/<ref>`).
#[must_use]
pub fn manifest_path(base: &str, namespace: &Namespace, reference: &Reference) -> String {
    format!("{base}{API_PREFIX}{namespace}{MANIFESTS}{reference}")
}

/// URL of a blob by digest (`/v2/<ns>/blobs/<digest>`).
#[must_use]
pub fn blob_path(base: &str, namespace: &Namespace, digest: &Digest) -> String {
    format!("{base}{API_PREFIX}{namespace}{BLOBS}{digest}")
}

/// URL that starts a resumable blob upload (`POST /v2/<ns>/blobs/uploads/`).
/// Session-continuation URLs are server-assigned via `Location`, never built
/// here.
#[must_use]
pub fn uploads_start_path(base: &str, namespace: &Namespace) -> String {
    format!("{base}{API_PREFIX}{namespace}{UPLOADS}")
}

/// URL of a tag listing, carrying the page size and cursor the request names.
#[must_use]
pub fn tags_list_path(base: &str, request: &ListTagsRequest) -> String {
    let namespace = &request.namespace;
    let location = format!("{base}{API_PREFIX}{namespace}{TAGS_LIST}");

    append_pagination(location, request.n, request.last.as_deref())
}

/// URL of a referrers listing, carrying the `artifactType` filter and the
/// cursor the request resumes from.
#[must_use]
pub fn referrers_path(base: &str, request: &GetReferrersRequest) -> String {
    let (namespace, digest) = (&request.namespace, &request.digest);
    let mut location = format!("{base}{API_PREFIX}{namespace}{REFERRERS}{digest}");
    if let Some(artifact_type) = &request.artifact_type {
        let filter = encode_query_value(artifact_type);
        location = append_query(&location, &format!("artifactType={filter}"));
    }

    append_pagination(location, None, request.last.as_deref())
}

/// Percent-encodes the characters a media type may hold that a query string
/// reads as syntax instead: `+` decodes back as a space wherever the reader
/// applies form decoding, `&` opens another parameter, and `#` opens a
/// fragment. A media type carries nothing else needing an escape.
fn encode_query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '+' => encoded.push_str("%2B"),
            '&' => encoded.push_str("%26"),
            '#' => encoded.push_str("%23"),
            _ => encoded.push(character),
        }
    }

    encoded
}

/// Appends a query string to a URL, choosing '?' or '&' for the separator.
#[must_use]
pub fn append_query(base: &str, query: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };

    format!("{base}{separator}{query}")
}

/// Appends the pagination a listing request carries, leaving the URL untouched
/// when it names neither a page size nor a cursor.
fn append_pagination(base: String, n: Option<u16>, last: Option<&str>) -> String {
    let mut location = base;
    if let Some(n) = n {
        location = append_query(&location, &format!("n={n}"));
    }
    if let Some(last) = last {
        location = append_query(&location, &format!("last={last}"));
    }

    location
}

/// The target of the `rel="next"` entry of a `Link` header value, or `None`
/// when the listing advertises no next page.
///
/// The unquoted `rel=next` form is accepted too: RFC 8288 allows it, and a
/// remote that spells it that way still has a next page to follow.
#[must_use]
pub fn next_page_target(header: &str) -> Option<&str> {
    header
        .split(',')
        .filter(|entry| has_rel_next(entry))
        .find_map(|entry| {
            let start = entry.find('<')?;
            let end = entry[start + 1..].find('>')? + start + 1;
            Some(&entry[start + 1..end])
        })
}

/// Whether a `Link` entry carries a `rel` param whose value is exactly `next`,
/// so a relation such as `nextpage` is not mistaken for one.
fn has_rel_next(entry: &str) -> bool {
    entry.split(';').any(|param| {
        param.split_once('=').is_some_and(|(name, value)| {
            name.trim().eq_ignore_ascii_case("rel") && value.trim().trim_matches('"') == "next"
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::next_page_link;
    use crate::types::{MediaType, Tag};

    const BASE: &str = "https://example.com";

    fn namespace() -> Namespace {
        Namespace::new("repo").unwrap()
    }

    #[test]
    fn manifest_and_blob_paths_address_by_reference_and_digest() {
        let digest = Digest::sha256_of_bytes(b"blob");
        assert_eq!(
            manifest_path(
                BASE,
                &namespace(),
                &Reference::Tag(Tag::new("latest").unwrap())
            ),
            "https://example.com/v2/repo/manifests/latest"
        );
        assert_eq!(
            blob_path(BASE, &namespace(), &digest),
            format!("https://example.com/v2/repo/blobs/{digest}")
        );
        assert_eq!(
            uploads_start_path(BASE, &namespace()),
            "https://example.com/v2/repo/blobs/uploads/"
        );
    }

    /// A listing that dropped `last` would restart the walk from the first page.
    #[test]
    fn a_tag_listing_carries_the_pagination_it_was_given() {
        let bare = tags_list_path(
            BASE,
            &ListTagsRequest {
                namespace: namespace(),
                n: None,
                last: None,
            },
        );
        assert_eq!(bare, "https://example.com/v2/repo/tags/list");

        let paged = tags_list_path(
            BASE,
            &ListTagsRequest {
                namespace: namespace(),
                n: Some(50),
                last: Some("v1.2.3".to_string()),
            },
        );
        assert_eq!(
            paged,
            "https://example.com/v2/repo/tags/list?n=50&last=v1.2.3"
        );
    }

    /// A listing that dropped `artifactType` would return entries the caller
    /// asked to filter out. The `+` of a structured-syntax suffix is escaped:
    /// left raw, a reader applying form decoding reads it back as a space and
    /// the media type no longer parses. No page size rides along: the spec
    /// defines none for this endpoint.
    #[test]
    fn a_referrers_listing_carries_the_filter_and_cursor() {
        let digest = Digest::sha256_of_bytes(b"subject");
        let path = referrers_path(
            BASE,
            &GetReferrersRequest {
                namespace: namespace(),
                digest: digest.clone(),
                artifact_type: Some(
                    MediaType::new("application/vnd.example.sbom.v1+json").unwrap(),
                ),
                last: Some("sha256:beef".to_string()),
            },
        );
        assert_eq!(
            path,
            format!(
                "https://example.com/v2/repo/referrers/{digest}?artifactType={filter}&last=sha256:beef",
                filter = "application/vnd.example.sbom.v1%2Bjson"
            )
        );
    }

    /// The other two characters a media type may hold that a query string would
    /// read as syntax: another parameter, and a fragment.
    #[test]
    fn a_filter_escapes_the_query_delimiters_it_may_carry() {
        assert_eq!(
            encode_query_value("application/vnd.a+b&c#d"),
            "application/vnd.a%2Bb%26c%23d"
        );
    }

    // The two halves of the `Link` header live on opposite sides of the
    // protocol, so what the registry renders is pinned against what the client
    // reads back.
    #[test]
    fn a_rendered_next_page_link_parses_back_to_its_target() {
        let target = "/v2/nginx/tags/list?n=100&last=v1.2.3";
        let header = next_page_link(target).unwrap();
        assert_eq!(next_page_target(header.to_str().unwrap()), Some(target));
    }

    #[test]
    fn the_unquoted_rel_form_is_accepted() {
        assert_eq!(
            next_page_target("</v2/nginx/tags/list?n=1>; rel=next"),
            Some("/v2/nginx/tags/list?n=1")
        );
    }

    #[test]
    fn only_the_next_entry_is_followed() {
        let header = "</first>; rel=\"prev\", </second>; rel=\"next\"";
        assert_eq!(next_page_target(header), Some("/second"));
    }

    #[test]
    fn a_rel_value_that_only_starts_with_next_is_not_followed() {
        assert_eq!(next_page_target("</first>; rel=nextpage"), None);
    }

    #[test]
    fn a_header_without_a_next_entry_has_no_target() {
        assert_eq!(next_page_target("</first>; rel=\"prev\""), None);
        assert_eq!(next_page_target("garbage"), None);
    }
}
