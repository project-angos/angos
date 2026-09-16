//! Parse an HTTP method and URL path into a Docker V2 extension [`Endpoint`].
//!
//! The only endpoint is the catalog, `GET /v2/_catalog`. A path this surface
//! does not serve returns `None`, so a caller can try the next surface.

use http::Method;
use serde::Deserialize;

/// A Docker Registry V2 extension operation and the query values it carries.
#[derive(Clone, Debug)]
pub enum Endpoint {
    /// `GET /v2/_catalog`.
    ListCatalog {
        n: Option<u16>,
        last: Option<String>,
    },
}

impl Endpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            Endpoint::ListCatalog { .. } => "list-catalog",
        }
    }
}

#[derive(Deserialize, Default)]
struct PaginationQuery {
    n: Option<u16>,
    last: Option<String>,
}

/// Parse `(method, path, query)` into a Docker extension [`Endpoint`], or
/// `None` when the path is not the catalog. `query` is the raw query without `?`.
#[must_use]
pub fn parse(method: &Method, path: &str, query: Option<&str>) -> Option<Endpoint> {
    if path == "/v2/_catalog" && *method == Method::GET {
        let PaginationQuery { n, last } =
            serde_html_form::from_str(query.unwrap_or_default()).ok()?;
        return Some(Endpoint::ListCatalog { n, last });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_parsed_with_pagination() {
        assert!(matches!(
            parse(&Method::GET, "/v2/_catalog", Some("n=5&last=foo")),
            Some(Endpoint::ListCatalog {
                n: Some(5),
                last: Some(_)
            })
        ));
        assert!(matches!(
            parse(&Method::GET, "/v2/_catalog", None),
            Some(Endpoint::ListCatalog {
                n: None,
                last: None
            })
        ));
    }

    #[test]
    fn declines_other() {
        assert!(parse(&Method::POST, "/v2/_catalog", None).is_none());
        assert!(parse(&Method::GET, "/v2/app/tags/list", None).is_none());
    }

    /// A page size angos cannot read is a bad cursor, not an absent one: serving
    /// an unpaginated listing instead would answer a different question.
    #[test]
    fn unreadable_n_is_not_a_route() {
        for query in ["n=abc", "n=65536"] {
            assert!(
                parse(&Method::GET, "/v2/_catalog", Some(query)).is_none(),
                "?{query} must not degrade into an unpaginated listing"
            );
        }

        // An empty value is form syntax for an absent one, which is a page size the
        // listing can answer.
        assert!(matches!(
            parse(&Method::GET, "/v2/_catalog", Some("n=")),
            Some(Endpoint::ListCatalog { n: None, .. })
        ));
    }

    #[test]
    fn url_encoded_last() {
        let route = parse(&Method::GET, "/v2/_catalog", Some("last=foo%2Fbar"));
        if let Some(Endpoint::ListCatalog { n, last }) = route {
            assert_eq!(n, None);
            assert_eq!(last, Some("foo/bar".to_string()));
        } else {
            panic!("Expected ListCatalog route");
        }
    }
}
