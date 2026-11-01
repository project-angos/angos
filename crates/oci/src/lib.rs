//! The OCI distribution vocabulary. [`types`] holds the validated values of
//! the protocol and [`request`] the shape of every operation, both spoken by
//! either end, [`response`] the answers they carry back; [`server`] reads a
//! request and renders the answer's headers, [`client`] builds the request and
//! reads those headers back.

pub mod client;
pub mod header;
pub mod path;
pub mod request;
pub mod response;
pub mod server;
pub mod types;

pub use types::{
    Algorithm, Content, DOCKER_REFERENCE_DIGEST, Descriptor, Digest, Error, IN_TOTO_PREDICATE_TYPE,
    Manifest, MediaRange, MediaType, Namespace, OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE,
    OCI_MANIFEST_SCHEMA_VERSION, Platform, Reference, Tag, UploadSessionId, constants, http_range,
    manifest_accept_types, namespace_belongs_to,
};
