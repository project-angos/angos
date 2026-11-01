use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::constants::DOCKER_REFERENCE_DIGEST;
use crate::types::{Digest, MediaType};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default)]
    #[serde(rename = "os.version")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    #[serde(default)]
    #[serde(rename = "os.features")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_features: Option<Vec<String>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub features: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    pub media_type: MediaType,
    pub digest: Digest,
    pub size: u64,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub annotations: HashMap<String, String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<MediaType>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
}

impl Descriptor {
    /// The subject this descriptor points at under the Docker-style reference
    /// digest annotation, or `None` when it carries none or one that is not a
    /// digest.
    #[must_use]
    pub fn docker_reference_subject(&self) -> Option<Digest> {
        self.annotations.get(DOCKER_REFERENCE_DIGEST)?.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn docker_reference_subject_is_none_without_the_annotation() {
        let mut descriptor = descriptor();
        descriptor
            .annotations
            .insert("other.key".into(), "v".into());
        assert_eq!(descriptor.docker_reference_subject(), None);
    }

    #[test]
    fn docker_reference_subject_is_none_when_it_is_not_a_digest() {
        let mut descriptor = descriptor();
        descriptor
            .annotations
            .insert(DOCKER_REFERENCE_DIGEST.into(), "not-a-valid-digest".into());
        assert_eq!(descriptor.docker_reference_subject(), None);
    }

    #[test]
    fn docker_reference_subject_parses_the_annotated_digest() {
        let subject = Digest::from_str(&format!("sha256:{}", "b".repeat(64))).unwrap();
        let mut descriptor = descriptor();
        descriptor
            .annotations
            .insert(DOCKER_REFERENCE_DIGEST.into(), subject.to_string());
        assert_eq!(descriptor.docker_reference_subject(), Some(subject));
    }

    fn descriptor() -> Descriptor {
        Descriptor {
            media_type: MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap(),
            digest: Digest::from_str(&format!("sha256:{}", "a".repeat(64))).unwrap(),
            size: 1,
            annotations: HashMap::new(),
            artifact_type: None,
            platform: None,
        }
    }
}
