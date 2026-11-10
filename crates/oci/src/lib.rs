//! The OCI distribution vocabulary shared by the registry and its clients.

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
    Platform, Reference, Tag, UploadSessionId, constants, http_range, manifest_accept_types,
    namespace_belongs_to,
};
