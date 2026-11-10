//! The Docker Registry V2 catalog, `GET /v2/_catalog`, which clients still call
//! but the OCI spec lacks; kept apart so the OCI service stays exactly the spec.

use serde::Deserialize;

use angos_oci::Namespace;

pub mod endpoint;
pub use endpoint::{Endpoint, parse};

#[cfg(feature = "hyper")]
mod render;

/// An absent `n` takes the server's default page size.
#[derive(Clone, Debug, Deserialize)]
pub struct CatalogRequest {
    pub n: Option<u16>,
    pub last: Option<String>,
}

/// One page of repository names, `next` naming the one to resume after.
#[cfg_attr(feature = "hyper", derive(serde::Serialize))]
pub struct Catalog {
    pub repositories: Vec<Namespace>,
    pub next: Option<String>,
}
