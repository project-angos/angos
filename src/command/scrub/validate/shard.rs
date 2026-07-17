//! Blob-index shard validation (absorbed from the old `BlobChecker`'s
//! per-entry probing). Runs after the link pass, so every grant a manifest
//! implies has been re-issued before entries are pruned against the links.

use std::collections::HashSet;

use tracing::{debug, warn};

use angos_tx_engine::StorageError;

use crate::{
    command::scrub::{
        action::{Action, WalkedStore},
        error::Error,
        validate::Validator,
    },
    oci::{Digest, Namespace},
    registry::{Error as RegistryError, metadata_store::LinkKind, path_builder},
};

impl Validator {
    /// Validate one `refs/{ns}.json` shard of `digest`'s blob index.
    pub async fn validate_shard(
        &self,
        key: &str,
        digest: &Digest,
        namespace_raw: &str,
    ) -> Result<(), Error> {
        let raw = match self.metadata_store.store().object_store().get(key).await {
            Ok(raw) => raw,
            Err(StorageError::NotFound) => return Ok(()),
            Err(e) => return Err(RegistryError::from(e).into()),
        };
        let Ok(links) = serde_json::from_slice::<HashSet<LinkKind>>(&raw) else {
            // Unreadable shard content: delete; the next run's link pass
            // re-grants every entry a manifest implies (a grant cannot land
            // on unreadable shard content this run). Hold the blob out of
            // this run's GC so the vanished references do not read as orphan.
            warn!("scrub: blob-index shard '{key}' does not parse; deleting");
            self.hold_blob_gc(digest);
            return self.delete_corrupt(WalkedStore::Metadata, key).await;
        };
        let Ok(namespace) = Namespace::new(namespace_raw) else {
            warn!("scrub: blob-index shard '{key}' names invalid namespace '{namespace_raw}'");
            return Ok(());
        };
        if links.is_empty() {
            // The write path deletes a shard when its set empties; a persisted
            // empty set is degenerate leftover.
            return self.delete_corrupt(WalkedStore::Metadata, key).await;
        }

        match self.blob_store.size(digest).await {
            Ok(_) => {
                for link in links {
                    // The blob's own self-link carries the grant itself.
                    if matches!(&link, LinkKind::Blob(owned) if owned == digest) {
                        continue;
                    }
                    // Only a confirmed-missing link file justifies removing
                    // the entry; a transient read error must not. The read is
                    // raw so the metadata cache cannot mask this run's repairs.
                    let link_key = path_builder::link_path(&link, &namespace);
                    match self
                        .metadata_store
                        .store()
                        .object_store()
                        .head(&link_key)
                        .await
                    {
                        Ok(_) => {}
                        Err(StorageError::NotFound) => {
                            let evidence = [key.to_string(), link_key.clone()];
                            let link_ref = &link;
                            let link_key = &link_key;
                            let reverify =
                                move || self.entry_still_dangling(key, link_ref, link_key);
                            if !self.confirm_repair(&evidence, reverify).await? {
                                continue;
                            }
                            self.emit(Action::RemoveBlobIndexLink {
                                namespace: namespace.clone(),
                                blob: digest.clone(),
                                link,
                            })
                            .await?;
                        }
                        Err(e) => return Err(RegistryError::from(e).into()),
                    }
                }
                Ok(())
            }
            Err(RegistryError::BlobUnknown | RegistryError::NotFound) => {
                // Bytes absent: an in-flight upload that granted before its
                // bytes landed, or a pull-through cache entry whose bytes are
                // fetched lazily. Normal state; the age-gated purge is
                // prune's job, so no warning here.
                debug!("scrub: blob-index shard '{key}' references byteless blob '{digest}'");
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Re-observe a dangling shard entry: the shard at `key` still holds
    /// `link` while its link file is still missing.
    async fn entry_still_dangling(
        &self,
        key: &str,
        link: &LinkKind,
        link_key: &str,
    ) -> Result<bool, Error> {
        let raw = match self.metadata_store.store().object_store().get(key).await {
            Ok(raw) => raw,
            Err(StorageError::NotFound) => return Ok(false),
            Err(e) => return Err(RegistryError::from(e).into()),
        };
        let Ok(links) = serde_json::from_slice::<HashSet<LinkKind>>(&raw) else {
            return Ok(false);
        };
        if !links.contains(link) {
            return Ok(false);
        }
        match self
            .metadata_store
            .store()
            .object_store()
            .get(link_key)
            .await
        {
            Ok(_) => Ok(false),
            Err(StorageError::NotFound) => Ok(true),
            Err(e) => Err(RegistryError::from(e).into()),
        }
    }
}
