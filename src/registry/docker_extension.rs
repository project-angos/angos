//! [`DockerExtensionService`] for [`Registry`]: the Docker V2 catalog, which
//! the OCI spec does not define.
//!
//! The trait is the transport's seam; the inherent [`Registry::list_catalog_entries`]
//! holds the paging and filtering, and this delegates to it.

use async_trait::async_trait;

use angos_docker_extension_service::{
    Catalog, CatalogRequest, DockerExtensionService, NamespaceVisibility,
};

use crate::registry::{Error, Registry};

#[async_trait]
impl DockerExtensionService for Registry {
    type Error = Error;

    async fn list_catalog(
        &self,
        request: CatalogRequest,
        visibility: &dyn NamespaceVisibility,
    ) -> Result<Catalog, Error> {
        self.list_catalog_entries(request, visibility).await
    }
}
