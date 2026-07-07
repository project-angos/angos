//! Accept-header set for downstream manifest probes. The last-writer-wins
//! wire constants live on the transport client (`crate::registry_client`),
//! which speaks them on the wire for every consumer.

use crate::oci::{
    DOCKER_MANIFEST_LIST_MEDIA_TYPE, DOCKER_MANIFEST_MEDIA_TYPE, OCI_INDEX_MEDIA_TYPE,
    OCI_MANIFEST_MEDIA_TYPE,
};

/// Manifest media types stamped as `Accept` headers on every downstream
/// manifest probe. Without them a content-negotiating registry may return a
/// converted representation whose digest never matches the local one.
#[must_use]
pub fn manifest_accept_types() -> Vec<String> {
    [
        OCI_MANIFEST_MEDIA_TYPE,
        OCI_INDEX_MEDIA_TYPE,
        DOCKER_MANIFEST_MEDIA_TYPE,
        DOCKER_MANIFEST_LIST_MEDIA_TYPE,
    ]
    .map(str::to_string)
    .to_vec()
}
