use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tracing::{info, instrument};

use super::Backend;
use crate::registry::{
    blob_store::{Error, MultipartCleanup},
    path_builder,
};

pub fn parse_upload_key(key: &str) -> Option<(String, String)> {
    let key = key.strip_prefix("v2/repositories/")?;
    let key = key.strip_suffix("/data")?;
    let (namespace, uuid) = key.rsplit_once("/_uploads/")?;
    Some((namespace.to_string(), uuid.to_string()))
}

impl Backend {
    async fn check_and_abort_orphan(
        &self,
        key: &str,
        upload_id: &str,
        initiated: DateTime<Utc>,
        now: DateTime<Utc>,
        timeout: Duration,
        dry_run: bool,
    ) -> Result<bool, Error> {
        if now.signed_duration_since(initiated) < timeout {
            return Ok(false);
        }
        let Some((namespace, uuid)) = parse_upload_key(key) else {
            return Ok(false);
        };
        let startedat_path = path_builder::upload_start_date_path(&namespace, &uuid);
        if self.store.object_size(&startedat_path).await.is_ok() {
            return Ok(false);
        }
        if dry_run {
            info!("DRY RUN: would abort orphan multipart upload {key}");
        } else {
            info!("Aborting orphan multipart upload {key}");
            self.store.abort_multipart_upload(key, upload_id).await?;
        }
        Ok(true)
    }
}

#[async_trait]
impl MultipartCleanup for Backend {
    #[instrument(skip(self))]
    async fn cleanup_orphan_multipart_uploads(
        &self,
        timeout: Duration,
        dry_run: bool,
    ) -> Result<usize, Error> {
        let mut count = 0;
        let now = Utc::now();
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;

        loop {
            let (uploads, next_key, next_upload_id) = self
                .store
                .list_multipart_uploads(None, key_marker.as_deref(), upload_id_marker.as_deref())
                .await?;

            for (key, upload_id, initiated) in uploads {
                if self
                    .check_and_abort_orphan(&key, &upload_id, initiated, now, timeout, dry_run)
                    .await?
                {
                    count += 1;
                }
            }

            if next_key.is_none() {
                break;
            }
            key_marker = next_key;
            upload_id_marker = next_upload_id;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_upload_key_valid() {
        let result = parse_upload_key("v2/repositories/my-repo/_uploads/abc-123-def/data");
        assert_eq!(
            result,
            Some(("my-repo".to_string(), "abc-123-def".to_string()))
        );
    }

    #[test]
    fn test_parse_upload_key_nested_namespace() {
        let result = parse_upload_key("v2/repositories/org/project/image/_uploads/uuid-here/data");
        assert_eq!(
            result,
            Some(("org/project/image".to_string(), "uuid-here".to_string()))
        );
    }

    #[test]
    fn test_parse_upload_key_invalid_prefix() {
        let result = parse_upload_key("invalid/prefix/_uploads/uuid/data");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_upload_key_invalid_suffix() {
        let result = parse_upload_key("v2/repositories/repo/_uploads/uuid/staged");
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_upload_key_missing_uploads() {
        let result = parse_upload_key("v2/repositories/repo/blobs/sha256/abc/data");
        assert_eq!(result, None);
    }
}
