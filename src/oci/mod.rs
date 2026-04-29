use serde::Serialize;

mod descriptor;
mod digest;
mod error;
mod manifest;
mod namespace;
mod reference;

pub use descriptor::{Descriptor, Platform};
pub use digest::Digest;
pub use error::Error;
pub use manifest::Manifest;
pub use namespace::{Namespace, namespace_belongs_to};
pub use reference::Reference;

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ReferrerList {
    pub schema_version: i32,
    pub media_type: String,
    pub manifests: Vec<Descriptor>,
}

impl Default for ReferrerList {
    fn default() -> Self {
        ReferrerList {
            schema_version: 2,
            media_type: "application/vnd.oci.image.index.v1+json".to_string(),
            manifests: Vec::new(),
        }
    }
}
