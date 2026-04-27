use super::{Backend, S3UploadState};
use crate::{cache::CacheExt, registry::blob_store::Error};

impl Backend {
    pub fn upload_id_cache_key(path: &str) -> String {
        format!("upload_id:{path}")
    }

    pub async fn get_or_search_upload_id(&self, path: &str) -> Result<Option<String>, Error> {
        if let Some(cache) = &self.cache
            && let Ok(Some(id)) = cache.retrieve_value(&Self::upload_id_cache_key(path)).await
        {
            return Ok(Some(id));
        }
        let id = self.store.search_multipart_upload_id(path).await?;
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
            let _ = cache.store(&key, state, 3600).await;
        }
    }

    pub async fn retrieve_cached_upload_state(
        &self,
        namespace: &str,
        uuid: &str,
    ) -> Option<S3UploadState> {
        if let Some(cache) = &self.cache {
            let key = Self::upload_state_cache_key(namespace, uuid);
            if let Ok(Some(state)) = cache.retrieve::<S3UploadState>(&key).await {
                return Some(state);
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
