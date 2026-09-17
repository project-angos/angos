//! Immutable revision and referrer records.
//!
//! A stored manifest is one never-mutated record at
//! [`NamespaceKeys::revision_record_path`] whose existence makes the digest
//! resolvable; a referrer is one record per (subject, referrer) whose body is
//! the referring manifest's descriptor.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use angos_oci::{Descriptor, Digest, MediaType, Namespace};
use angos_storage::Error as StorageError;

use crate::registry::{
    Error,
    keys::NamespaceKeys,
    metadata_store::{LinkMetadata, MetadataStore, stored_media_type},
};

/// The stored body of a revision record: written once and never mutated, so
/// a re-push of a stored digest keeps the record it already has.
#[derive(Debug, Serialize, Deserialize)]
pub struct RevisionRecord {
    #[serde(
        default,
        deserialize_with = "stored_media_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub media_type: Option<MediaType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

impl MetadataStore {
    /// Write the revision record with one create-if-absent, returning whether
    /// it was created: the record is never mutated, so a re-push of a stored
    /// digest keeps the record it already has, `created_at` included. A
    /// replicated write stamps its author's time, anything else this
    /// replica's clock.
    pub async fn put_revision(
        &self,
        namespace: &Namespace,
        digest: &Digest,
        media_type: Option<MediaType>,
        authored_at: Option<DateTime<Utc>>,
    ) -> Result<bool, Error> {
        let body = serde_json::to_vec(&RevisionRecord {
            media_type,
            created_at: Some(authored_at.unwrap_or_else(Utc::now)),
        })?;
        let created = self
            .object_store()
            .create_if_absent(&namespace.revision_record_path(digest), Bytes::from(body))
            .await?;
        if created {
            self.index_namespace(namespace).await;
        }
        Ok(created)
    }

    /// Write the referrer record with one create-if-absent. The body is the
    /// referring manifest's descriptor, or an empty object when the push
    /// carried none (the listing needs only the key).
    pub async fn put_referrer(
        &self,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
        descriptor: Option<&Descriptor>,
    ) -> Result<(), Error> {
        let body = match descriptor {
            Some(descriptor) => serde_json::to_vec(descriptor)?,
            None => b"{}".to_vec(),
        };
        self.object_store()
            .create_if_absent(
                &namespace.referrer_record_path(subject, referrer),
                Bytes::from(body),
            )
            .await?;
        Ok(())
    }

    /// Delete the referrer record; a record already gone is a no-op.
    pub async fn delete_referrer(
        &self,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<(), Error> {
        self.object_store()
            .delete(&namespace.referrer_record_path(subject, referrer))
            .await?;
        Ok(())
    }

    /// Resolve a manifest revision to link-shaped metadata.
    pub async fn resolve_revision(
        &self,
        namespace: &Namespace,
        digest: &Digest,
    ) -> Result<LinkMetadata, Error> {
        let key = namespace.revision_record_path(digest);
        match self.object_store().get(&key).await {
            Ok(body) => {
                let record: RevisionRecord = serde_json::from_slice(&body)
                    .map_err(|e| Error::Internal(format!("corrupt revision record {key}: {e}")))?;
                Ok(LinkMetadata {
                    target: digest.clone(),
                    created_at: record.created_at,
                    media_type: record.media_type,
                    descriptor: None,
                })
            }
            Err(StorageError::NotFound) => Err(Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Resolve a referrer back-link to its stored descriptor.
    pub async fn resolve_referrer(
        &self,
        namespace: &Namespace,
        subject: &Digest,
        referrer: &Digest,
    ) -> Result<LinkMetadata, Error> {
        let key = namespace.referrer_record_path(subject, referrer);
        match self.object_store().get(&key).await {
            Ok(body) => Ok(LinkMetadata {
                target: referrer.clone(),
                created_at: None,
                media_type: None,
                descriptor: serde_json::from_slice::<Descriptor>(&body).ok(),
            }),
            Err(StorageError::NotFound) => Err(Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }
}
