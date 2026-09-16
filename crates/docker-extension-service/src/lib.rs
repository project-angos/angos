//! The Docker Registry V2 extensions the OCI Distribution spec does not define.
//!
//! Currently just the catalog, `GET /v2/_catalog`: a listing the Docker V2 API
//! carried and clients still call, absent from the OCI spec. Kept as its own
//! trait so the OCI service stays exactly the spec and this stays clearly an
//! extension.

use async_trait::async_trait;

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

/// Whether the authenticated caller may see a namespace in a listing.
///
/// Catalog authorization is the transport's, not the service's: the transport
/// implements this over its authorizer and the service consults it while
/// paging, so the `Link` cursor advances over entries filtered out rather than
/// short pages hiding the continuation. Blanket-implemented for any
/// `Fn(&Namespace) -> bool`, so a caller may pass a closure where a
/// `&dyn NamespaceVisibility` is expected.
pub trait NamespaceVisibility: Send + Sync {
    fn allows(&self, namespace: &Namespace) -> bool;
}

impl<F: Fn(&Namespace) -> bool + Send + Sync> NamespaceVisibility for F {
    fn allows(&self, namespace: &Namespace) -> bool {
        self(namespace)
    }
}

/// The Docker V2 catalog extension.
#[async_trait]
pub trait DockerExtensionService: Send + Sync {
    /// The implementor's error, rendered as an OCI error at the transport edge.
    type Error;

    /// `GET /v2/_catalog`. `visibility` decides which repositories the caller
    /// may see; it is consulted per entry so pagination stays correct. The
    /// caller's identity rides in `visibility`, so no actor is passed.
    async fn list_catalog(
        &self,
        request: CatalogRequest,
        visibility: &dyn NamespaceVisibility,
    ) -> Result<Catalog, Self::Error>;
}
