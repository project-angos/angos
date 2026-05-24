use tracing::warn;

use crate::registry::blob_store::{
    Error,
    s3::{Backend, S3UploadState},
};
use angos_storage::{MultipartStore, UploadId};

impl Backend {
    pub fn upload_id_cache_key(path: &str) -> String {
        format!("upload_id:{path}")
    }

    /// Returns the upload id for `path`, consulting the cache first and falling
    /// back to `search_multipart_upload_id` on the store. The result is a plain
    /// `String` so the cache layer stays independent of the `UploadId` newtype.
    pub async fn get_or_search_upload_id(&self, path: &str) -> Result<Option<String>, Error> {
        if let Some(cache) = &self.cache
            && let Ok(Some(id)) = cache.retrieve_value(&Self::upload_id_cache_key(path)).await
        {
            return Ok(Some(id));
        }
        let id = self
            .store
            .search_multipart_upload_id(path)
            .await?
            .map(UploadId::into_inner);
        if let Some(ref upload_id) = id {
            self.cache_upload_id(path, upload_id).await;
        }
        Ok(id)
    }

    pub async fn cache_upload_id(&self, path: &str, upload_id: &str) {
        if let Some(cache) = &self.cache {
            let _ = cache
                .store_value(&Self::upload_id_cache_key(path), upload_id, 3600)
                .await;
        }
    }

    pub async fn evict_upload_id(&self, path: &str) {
        if let Some(cache) = &self.cache {
            let _ = cache.delete_value(&Self::upload_id_cache_key(path)).await;
        }
    }

    fn upload_state_cache_key(namespace: &str, uuid: &str) -> String {
        format!("upload_state:{namespace}:{uuid}")
    }

    pub async fn cache_upload_state(&self, namespace: &str, uuid: &str, state: &S3UploadState) {
        if let Some(cache) = &self.cache {
            let key = Self::upload_state_cache_key(namespace, uuid);
            if let Err(err) = cache.store(&key, state, 3600).await {
                warn!("Failed to cache upload state for {namespace}/{uuid}: {err}");
            }
        }
    }

    pub async fn retrieve_cached_upload_state(
        &self,
        namespace: &str,
        uuid: &str,
    ) -> Option<S3UploadState> {
        if let Some(cache) = &self.cache {
            let key = Self::upload_state_cache_key(namespace, uuid);
            match cache.retrieve::<S3UploadState>(&key).await {
                Ok(Some(state)) => return Some(state),
                Err(err) => {
                    warn!("Failed to retrieve cached upload state for {namespace}/{uuid}: {err}");
                }
                Ok(None) => {}
            }
        }
        None
    }

    pub async fn evict_upload_state(&self, namespace: &str, uuid: &str) {
        if let Some(cache) = &self.cache {
            let key = Self::upload_state_cache_key(namespace, uuid);
            let _ = cache.delete_value(&key).await;
        }
    }
}
