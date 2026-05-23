//! Filesystem-backed [`ObjectStore`].
//!
//! Maps the storage trait surface onto a directory tree rooted at `root_dir`:
//! every object key becomes a relative path under the root. Writes are atomic
//! (temp-file + rename) and `delete_prefix` walks the subtree. Listings sort
//! lexicographically because `read_dir` returns entries in arbitrary order.
//!
//! Does **not** implement [`ConditionalStore`](crate::ConditionalStore) or
//! [`MultipartStore`](crate::MultipartStore): on FS those concerns are handled
//! one layer up via the metadata store's lock backend and the blob store's
//! append-mode upload path. See `doc/storage-convergence.md`.

use std::{
    fs::FileType,
    io::{self, ErrorKind, SeekFrom, Write},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use bytes::Bytes;
use tempfile::NamedTempFile;
use tokio::{fs, io::AsyncSeekExt, task::spawn_blocking};

use crate::{BoxedReader, ChildrenPage, Error, ObjectMeta, ObjectStore, Page};

/// Builder for [`Backend`].
pub struct Builder {
    root: Option<PathBuf>,
    sync_to_disk: bool,
}

impl Builder {
    fn new() -> Self {
        Self {
            root: None,
            sync_to_disk: false,
        }
    }

    /// Directory under which all object keys are resolved. Required.
    pub fn root_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.root = Some(root.into());
        self
    }

    /// When `true`, every write `fsync`s before the temp-file rename. Adds
    /// durability at the cost of a syscall per write.
    pub fn sync_to_disk(mut self, sync: bool) -> Self {
        self.sync_to_disk = sync;
        self
    }

    pub fn build(self) -> Result<Backend, Error> {
        let root = self
            .root
            .ok_or_else(|| Error::Backend("fs::Backend requires root_dir".to_string()))?;
        Ok(Backend {
            root,
            sync_to_disk: self.sync_to_disk,
        })
    }
}

/// Filesystem [`ObjectStore`] implementation.
#[derive(Clone, Debug)]
pub struct Backend {
    root: PathBuf,
    sync_to_disk: bool,
}

impl Backend {
    pub fn builder() -> Builder {
        Builder::new()
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

async fn ensure_parent(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    Ok(())
}

async fn atomic_write(target: &Path, data: Bytes, sync: bool) -> Result<(), Error> {
    ensure_parent(target).await?;
    let parent = target.parent().unwrap_or_else(|| Path::new(".")).to_owned();
    let final_path = target.to_owned();
    spawn_blocking(move || -> io::Result<()> {
        let mut temp = NamedTempFile::new_in(&parent)?;
        temp.write_all(&data)?;
        if sync {
            temp.flush()?;
            temp.as_file().sync_all()?;
        }
        temp.persist(final_path).map_err(|e| e.error)?;
        Ok(())
    })
    .await
    .map_err(|e| Error::Backend(format!("temp-write task panicked: {e}")))??;
    Ok(())
}

/// Sorted list of immediate entries; missing dir yields an empty vector.
async fn read_dir_sorted(path: &Path) -> Result<Vec<(String, FileType)>, Error> {
    let mut reader = match fs::read_dir(path).await {
        Ok(r) => r,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut entries = Vec::new();
    while let Some(entry) = reader.next_entry().await? {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let file_type = entry.file_type().await?;
        entries.push((name, file_type));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
}

/// Recursively walk `dir` and collect every regular-file key relative to
/// `dir` itself. Sorted lexicographically. Matches the S3 backend, which
/// returns prefix-relative names from `ListObjectsV2`.
async fn collect_flat_keys(dir: &Path) -> Result<Vec<String>, Error> {
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];
    let mut keys = Vec::new();
    while let Some(current) = stack.pop() {
        let entries = read_dir_sorted(&current).await?;
        // Reverse so pop() yields in sorted order.
        for (name, file_type) in entries.into_iter().rev() {
            let child = current.join(&name);
            if file_type.is_dir() {
                stack.push(child);
            } else if let Ok(rel) = child.strip_prefix(dir)
                && let Some(rel_str) = rel.to_str()
            {
                keys.push(rel_str.to_string());
            }
        }
    }
    keys.sort();
    Ok(keys)
}

#[async_trait]
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        Ok(fs::read(self.full_path(key)).await?)
    }

    async fn get_stream(
        &self,
        key: &str,
        offset: Option<u64>,
    ) -> Result<(BoxedReader, u64), Error> {
        let mut file = fs::File::open(self.full_path(key)).await?;
        let total = file.metadata().await?.len();
        if let Some(start) = offset {
            file.seek(SeekFrom::Start(start)).await?;
        }
        Ok((Box::new(file), total))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), Error> {
        atomic_write(&self.full_path(key), data, self.sync_to_disk).await
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        match fs::remove_file(self.full_path(key)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        let path = self.full_path(prefix);
        // Directory → recursive remove; file → unlink; missing → success.
        match fs::metadata(&path).await {
            Ok(meta) if meta.is_dir() => match fs::remove_dir_all(&path).await {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            },
            Ok(_) => self.delete(prefix).await,
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, Error> {
        let meta = fs::metadata(self.full_path(key)).await?;
        let last_modified = meta.modified().ok().map(Into::into);
        Ok(ObjectMeta {
            size: meta.len(),
            etag: None,
            last_modified,
        })
    }

    async fn list(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
    ) -> Result<Page<String>, Error> {
        let all_keys = collect_flat_keys(&self.full_path(prefix)).await?;
        let start = token.as_deref().map_or(0, |t| {
            all_keys
                .iter()
                .position(|k| k.as_str() > t)
                .unwrap_or(all_keys.len())
        });
        let end = (start + n as usize).min(all_keys.len());
        let items: Vec<String> = all_keys[start..end].to_vec();
        let next_token = (end < all_keys.len())
            .then(|| items.last().cloned())
            .flatten();
        Ok(Page { items, next_token })
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        let entries = read_dir_sorted(&self.full_path(prefix)).await?;

        // Token wins over start_after when both are set: token is what the
        // backend itself emitted, start_after is caller-supplied.
        let cursor = token.as_deref().or(start_after.as_deref());
        let lower = cursor.map_or(0, |t| {
            entries
                .iter()
                .position(|(name, _)| name.as_str() > t)
                .unwrap_or(entries.len())
        });
        let upper = (lower + n as usize).min(entries.len());
        let slice = &entries[lower..upper];

        let mut sub_prefixes = Vec::new();
        let mut objects = Vec::new();
        for (name, file_type) in slice {
            if file_type.is_dir() {
                sub_prefixes.push(name.clone());
            } else {
                objects.push(name.clone());
            }
        }

        let next_token = (upper < entries.len())
            .then(|| slice.last().map(|(name, _)| name.clone()))
            .flatten();

        Ok(ChildrenPage {
            sub_prefixes,
            objects,
            next_token,
        })
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), Error> {
        let data = fs::read(self.full_path(source)).await?;
        atomic_write(
            &self.full_path(destination),
            Bytes::from(data),
            self.sync_to_disk,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
