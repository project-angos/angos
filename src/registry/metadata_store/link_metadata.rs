use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::oci::Digest;
use crate::registry::metadata_store::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkMetadata {
    pub target: Digest,
    pub created_at: Option<DateTime<Utc>>,
    pub accessed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub referenced_by: HashSet<Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

impl LinkMetadata {
    pub fn from_digest(target: Digest) -> Self {
        Self {
            target,
            created_at: Some(Utc::now()),
            accessed_at: None,
            referenced_by: HashSet::new(),
            media_type: None,
        }
    }

    pub fn add_referrer(&mut self, digest: Digest) {
        self.referenced_by.insert(digest);
    }

    pub fn remove_referrer(&mut self, digest: &Digest) {
        self.referenced_by.remove(digest);
    }

    pub fn has_references(&self) -> bool {
        !self.referenced_by.is_empty()
    }

    pub fn from_bytes(s: Vec<u8>) -> Result<Self, Error> {
        // Try to deserialize the data as LinkMetadata, if it fails, it means
        // the link is a simple string (probably coming from "distribution" implementation).
        if let Ok(metadata) = serde_json::from_slice(&s) {
            Ok(metadata)
        } else {
            // If the link is not a valid LinkMetadata, we create a new one
            let target = String::from_utf8(s).map_err(|e| Error::InvalidData(e.to_string()))?;
            let target =
                Digest::try_from(target.as_str()).map_err(|e| Error::InvalidData(e.to_string()))?;

            Ok(LinkMetadata {
                target,
                created_at: Some(Utc::now()),
                accessed_at: None,
                referenced_by: HashSet::new(),
                media_type: None,
            })
        }
    }

    pub fn with_media_type(mut self, media_type: Option<String>) -> Self {
        self.media_type = media_type;
        self
    }

    pub fn accessed(mut self) -> Self {
        self.accessed_at = Some(Utc::now());
        self
    }
}
