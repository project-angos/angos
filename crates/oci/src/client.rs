//! The requesting side: request URLs, and reading answer headers back.

use crate::path::{API_PREFIX, BLOBS, MANIFESTS, REFERRERS, TAGS_LIST, UPLOADS};
use crate::request::{GetReferrersRequest, ListTagsRequest};
use crate::types::{Digest, Namespace, Reference};

#[must_use]
pub fn manifest_path(base: &str, namespace: &Namespace, reference: &Reference) -> String {
    format!("{base}{API_PREFIX}{namespace}{MANIFESTS}{reference}")
}

#[must_use]
pub fn blob_path(base: &str, namespace: &Namespace, digest: &Digest) -> String {
    format!("{base}{API_PREFIX}{namespace}{BLOBS}{digest}")
}

/// Session-continuation URLs are server-assigned, never built here.
#[must_use]
pub fn uploads_start_path(base: &str, namespace: &Namespace) -> String {
    format!("{base}{API_PREFIX}{namespace}{UPLOADS}")
}

#[must_use]
pub fn tags_list_path(base: &str, request: &ListTagsRequest) -> String {
    let namespace = &request.namespace;
    let location = format!("{base}{API_PREFIX}{namespace}{TAGS_LIST}");

    append_pagination(location, request.n, request.last.as_deref())
}

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

/// Escapes what a media type may hold that a query reads as syntax: `+` (a
/// space under form decoding), `&` and `#`.
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

#[must_use]
pub fn append_query(base: &str, query: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };

    format!("{base}{separator}{query}")
}

fn append_pagination(mut location: String, n: Option<u16>, last: Option<&str>) -> String {
    if let Some(n) = n {
        location = append_query(&location, &format!("n={n}"));
    }
    if let Some(last) = last {
        location = append_query(&location, &format!("last={last}"));
    }

    location
}

/// The target of the `rel="next"` entry of a `Link` header; RFC 8288 also
/// allows it unquoted.
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

fn has_rel_next(entry: &str) -> bool {
    entry.split(';').any(|param| {
        param.split_once('=').is_some_and(|(name, value)| {
            name.trim().eq_ignore_ascii_case("rel") && value.trim().trim_matches('"') == "next"
        })
    })
}

#[cfg(test)]
mod tests {
    use http::header::LINK;

    use super::*;
    use crate::server::paginated_json_headers;
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

    /// No page size rides along: the spec defines none for this endpoint.
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

    #[test]
    fn a_filter_escapes_the_query_delimiters_it_may_carry() {
        assert_eq!(
            encode_query_value("application/vnd.a+b&c#d"),
            "application/vnd.a%2Bb%26c%23d"
        );
    }

    #[test]
    fn a_rendered_next_page_link_parses_back_to_its_target() {
        let target = "/v2/nginx/tags/list?n=100&last=v1.2.3";
        let headers = paginated_json_headers(Some(target)).unwrap();
        assert_eq!(
            next_page_target(headers[&LINK].to_str().unwrap()),
            Some(target)
        );
    }

    #[test]
    fn only_an_exact_rel_next_is_followed() {
        for (header, target) in [
            (
                "</v2/nginx/tags/list?n=1>; rel=next",
                Some("/v2/nginx/tags/list?n=1"),
            ),
            (
                "</first>; rel=\"prev\", </second>; rel=\"next\"",
                Some("/second"),
            ),
            ("</first>; rel=nextpage", None),
            ("</first>; rel=\"prev\"", None),
            ("garbage", None),
        ] {
            assert_eq!(next_page_target(header), target, "{header}");
        }
    }
}
