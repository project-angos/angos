//! Routing for the Docker V2 extension surface: the catalog only.

use http::Method;

use crate::CatalogRequest;

#[derive(Clone, Debug)]
pub enum Endpoint {
    /// `GET /v2/_catalog`.
    ListCatalog(CatalogRequest),
}

impl Endpoint {
    /// A stable, per-endpoint name for logs and metrics.
    #[must_use]
    pub fn endpoint_name(&self) -> &'static str {
        match self {
            Endpoint::ListCatalog(_) => "list-catalog",
        }
    }
}

/// `None` when the path is not the catalog, so a caller can try the next
/// surface. `query` is the raw query without `?`.
#[must_use]
pub fn parse(method: &Method, path: &str, query: Option<&str>) -> Option<Endpoint> {
    if path == "/v2/_catalog" && *method == Method::GET {
        return serde_html_form::from_str(query.unwrap_or_default())
            .ok()
            .map(Endpoint::ListCatalog);
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
            Some(Endpoint::ListCatalog(CatalogRequest {
                n: Some(5),
                last: Some(_)
            }))
        ));
        assert!(matches!(
            parse(&Method::GET, "/v2/_catalog", None),
            Some(Endpoint::ListCatalog(CatalogRequest {
                n: None,
                last: None
            }))
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
            Some(Endpoint::ListCatalog(CatalogRequest { n: None, .. }))
        ));
    }
}
