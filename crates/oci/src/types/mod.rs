//! The validated value types of the distribution protocol.

pub mod constants;
pub mod http_range;

#[macro_use]
mod str_newtype;

mod descriptor;
mod digest;
mod error;
mod manifest;
mod media_range;
mod media_type;
mod namespace;
mod reference;
mod tag;
mod upload_session_id;

pub use constants::{
    DOCKER_REFERENCE_DIGEST, IN_TOTO_PREDICATE_TYPE, OCI_INDEX_MEDIA_TYPE, OCI_MANIFEST_MEDIA_TYPE,
};
pub use descriptor::{Descriptor, Platform};
pub use digest::{Algorithm, Digest};
pub use error::Error;
pub use manifest::{Content, Manifest};
pub use media_range::{MediaRange, manifest_accept_types};
pub use media_type::MediaType;
pub use namespace::{Namespace, namespace_belongs_to};
pub use reference::Reference;
pub use tag::Tag;
pub use upload_session_id::UploadSessionId;
