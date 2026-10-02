//! The Docker Registry V2 extensions the OCI Distribution spec does not define.
//!
//! Currently just the catalog, `GET /v2/_catalog`: a listing the Docker V2 API
//! carried and clients still call, absent from the OCI spec. Kept as its own
//! crate so the OCI service stays exactly the spec and this stays clearly an
//! extension.

use angos_oci::Namespace;

pub mod endpoint;
pub use endpoint::{Endpoint, parse};

#[cfg(feature = "hyper")]
mod render;

/// `GET /v2/_catalog` query: a page size and the name to resume after, both
/// optional. An absent `n` takes the server's default page size.
pub struct CatalogRequest {
    pub n: Option<u16>,
    pub last: Option<String>,
}

/// One page of repository names the caller may see, and the name to resume
/// after when the listing has more. `next` is `None` once exhausted, so
/// "there is another page but no cursor" cannot be represented.
#[cfg_attr(feature = "hyper", derive(serde::Serialize))]
pub struct Catalog {
    pub repositories: Vec<Namespace>,
    pub next: Option<String>,
}
