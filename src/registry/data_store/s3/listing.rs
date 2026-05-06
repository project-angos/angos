//! Paginated listing operations for S3 object keys and common prefixes.

use std::io::Error as IoError;

use super::Backend;

impl Backend {
    pub async fn list_prefixes(
        &self,
        path: &str,
        delimiter: &str,
        max_keys: u16,
        continuation_token: Option<String>,
        start_after: Option<String>,
    ) -> Result<(Vec<String>, Vec<String>, Option<String>), IoError> {
        self.check_circuit_breaker()?;
        let mut full_prefix = self.full_key(path);
        if !full_prefix.is_empty() && !full_prefix.ends_with('/') {
            full_prefix.push('/');
        }

        let full_start_after = start_after.map(|s| format!("{full_prefix}{s}{delimiter}"));

        let result = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .delimiter(delimiter)
            .max_keys(i32::from(max_keys))
            .set_continuation_token(continuation_token)
            .set_start_after(full_start_after)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()));
        self.record_io_result(&result);
        let res = result?;

        let mut prefixes = Vec::new();
        for prefix in res.common_prefixes.unwrap_or_default() {
            if let Some(p) = prefix.prefix
                && let Some(name) = p.strip_prefix(&full_prefix)
            {
                let name = name
                    .strip_suffix(delimiter)
                    .unwrap_or(name)
                    .trim_start_matches('/');
                prefixes.push(name.to_string());
            }
        }

        let mut objects = Vec::new();
        for object in res.contents.unwrap_or_default() {
            if let Some(key) = object.key
                && let Some(name) = key.strip_prefix(&full_prefix)
            {
                objects.push(name.to_string());
            }
        }

        let next_token = if res.is_truncated.unwrap_or(false) {
            res.next_continuation_token
        } else {
            None
        };

        Ok((prefixes, objects, next_token))
    }

    pub async fn list_objects(
        &self,
        path: &str,
        max_keys: u16,
        continuation_token: Option<String>,
    ) -> Result<(Vec<String>, Option<String>), IoError> {
        let mut full_prefix = self.full_key(path);
        if !full_prefix.is_empty() && !full_prefix.ends_with('/') {
            full_prefix.push('/');
        }

        let res = self
            .s3_client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .max_keys(i32::from(max_keys))
            .set_continuation_token(continuation_token)
            .send()
            .await
            .map_err(|e| IoError::other(e.to_string()))?;

        let mut objects = Vec::new();
        for object in res.contents.unwrap_or_default() {
            if let Some(key) = object.key {
                let relative = if let Some(stripped) = key.strip_prefix(&full_prefix) {
                    stripped.trim_start_matches('/')
                } else {
                    &key
                };
                objects.push(relative.to_string());
            }
        }

        let next_token = if res.is_truncated.unwrap_or(false) {
            res.next_continuation_token
        } else {
            None
        };

        Ok((objects, next_token))
    }
}
