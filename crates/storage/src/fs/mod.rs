//! Filesystem-backed [`ObjectStore`] and [`UploadSessionStore`].
//!
//! Maps the storage trait surface onto a directory tree rooted at `root_dir`:
//! every object key becomes a relative path under the root. Writes are atomic
//! (temp-file + rename) and `delete_prefix` walks the subtree. Listings sort
//! lexicographically because `read_dir` returns entries in arbitrary order.
//!
//! Does **not** implement [`ConditionalStore`](crate::ConditionalStore): on
//! FS conditional updates are handled one layer up via the metadata store's
//! lock backend.
//!
//! Upload sessions use an append-mode file at the session's `key` — no
//! staging artifacts, no multipart protocol. `complete_upload` is a no-op
//! because the data is already at `key`; the caller's transactional move
//! to the canonical location is the only finalisation step.

use std::{
    fs::FileType,
    io::{self, ErrorKind, SeekFrom, Write},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use tempfile::Builder as TempFileBuilder;
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
    task::spawn_blocking,
};
use tokio_util::io::StreamReader;

use crate::{
    BoxedReader, ByteStream, ChildrenPage, Error, ObjectMeta, ObjectStore, Page, SessionState,
    UploadSession, UploadSessionStore,
};

/// Page step used by [`Backend::list_all_children`] when draining all pages
/// of a directory listing. 512 is large enough to complete most namespaces in
/// a single round-trip while staying well within the OS read-dir buffer limits.
/// It is an internal implementation detail and is not user-tuneable.
const LIST_ALL_CHILDREN_PAGE_SIZE: u16 = 512;

/// Filename prefix for the temp files [`atomic_write`] creates next to their
/// target before the rename. These are an implementation detail of atomic
/// writes and must never be surfaced as objects: a concurrent listing+read
/// could otherwise observe a freshly-created, still-empty temp file and, for a
/// JSON object key, fail to parse it. Listings skip any entry with this prefix.
const ATOMIC_WRITE_TMP_PREFIX: &str = ".angos-write.";

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
    #[must_use]
    pub fn root_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.root = Some(root.into());
        self
    }

    /// When `true`, every write `fsync`s before the temp-file rename. Adds
    /// durability at the cost of a syscall per write.
    #[must_use]
    pub fn sync_to_disk(mut self, sync: bool) -> Self {
        self.sync_to_disk = sync;
        self
    }

    /// # Errors
    /// Returns [`Error::Backend`] when [`root_dir`](Self::root_dir) was
    /// never called.
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
    #[must_use]
    pub fn builder() -> Builder {
        Builder::new()
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// Atomically rename the object at `from_key` to `to_key`, creating
    /// the destination's parent directories as needed.
    ///
    /// This is a zero-copy operation on the same filesystem. It is intentionally
    /// not on the [`ObjectStore`] trait because S3 has no cheap rename.
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] when `from_key` does not exist.
    pub async fn rename(&self, from_key: &str, to_key: &str) -> Result<(), Error> {
        let from = self.full_path(from_key);
        let to = self.full_path(to_key);
        ensure_parent(&to).await?;
        fs::rename(&from, &to).await?;
        Ok(())
    }

    /// Open `key` for writing, optionally in append mode.
    ///
    /// When `append` is `true` the file is opened with `O_APPEND`; when
    /// `false` the file is truncated and overwritten. The parent directory is
    /// created if it does not exist.
    ///
    /// Returns the open file handle and the file's size **before** the open,
    /// so callers that need to track the write offset can do so without a
    /// separate `head` call.
    ///
    /// # Errors
    /// Returns [`Error::NotFound`] when the file does not exist and `append`
    /// is `true`.
    pub async fn open_for_write(&self, key: &str, append: bool) -> Result<(fs::File, u64), Error> {
        let path = self.full_path(key);
        ensure_parent(&path).await?;
        if append {
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await
                .map_err(|e| classify_open_error(&e, &path))?;
            let size = file.metadata().await?.len();
            Ok((file, size))
        } else {
            let file = fs::File::create(&path)
                .await
                .map_err(|e| classify_open_error(&e, &path))?;
            Ok((file, 0))
        }
    }

    /// Best-effort cleanup of empty ancestor directories after a leaf
    /// deletion. Walks at most `max_levels` parents upward; stops at the
    /// first non-empty parent, at `root_dir` (exclusive), or as soon as it
    /// would leave the `root_dir` subtree. Errors are suppressed.
    ///
    /// Inherent (not trait-method) because directory pruning is meaningless
    /// on S3 and the blob store's unified Backend never needs to call it
    /// through the trait surface.
    pub async fn prune_empty_ancestors(&self, key: &str, max_levels: u8) {
        let start = self.full_path(key);
        let root = &self.root;
        let mut current = start.parent();
        for _ in 0..max_levels {
            let Some(parent) = current else { break };
            if parent == root || !parent.starts_with(root) {
                break;
            }
            let Ok(mut entries) = fs::read_dir(parent).await else {
                break;
            };
            match entries.next_entry().await {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) => {}
            }
            if fs::remove_dir(parent).await.is_err() {
                break;
            }
            current = parent.parent();
        }
    }

    /// Enumerate every immediate child name under `prefix`, looping through
    /// all pages internally until `next_token` is `None`.
    ///
    /// Returns separate lists of sub-prefix names and object names,
    /// both sorted lexicographically. A missing or empty directory yields
    /// empty lists without an error.
    ///
    /// Use this instead of a single `list_children` call whenever you need
    /// complete enumeration and the number of children is unbounded.
    ///
    /// # Errors
    /// Propagates any I/O error from the underlying directory reads.
    pub async fn list_all_children(
        &self,
        prefix: &str,
    ) -> Result<(Vec<String>, Vec<String>), Error> {
        let mut all_sub_prefixes: Vec<String> = Vec::new();
        let mut all_objects: Vec<String> = Vec::new();
        let mut token: Option<String> = None;

        loop {
            let page = self
                .list_children(prefix, LIST_ALL_CHILDREN_PAGE_SIZE, token, None)
                .await?;
            all_sub_prefixes.extend(page.sub_prefixes);
            all_objects.extend(page.objects);
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }

        all_sub_prefixes.sort();
        all_objects.sort();
        Ok((all_sub_prefixes, all_objects))
    }
}

/// Create all parent directories of `path`.
async fn ensure_parent(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    Ok(())
}

/// Map a file-open error to an [`Error`], logging the full path for diagnostics.
fn classify_open_error(error: &io::Error, path: &Path) -> Error {
    if error.kind() == ErrorKind::NotFound {
        Error::NotFound
    } else {
        Error::Backend(format!("could not open {}: {error}", path.display()))
    }
}

async fn atomic_write(target: &Path, data: Bytes, sync: bool) -> Result<(), Error> {
    ensure_parent(target).await?;
    let parent = target.parent().unwrap_or_else(|| Path::new(".")).to_owned();
    let final_path = target.to_owned();
    spawn_blocking(move || -> io::Result<()> {
        let mut temp = TempFileBuilder::new()
            .prefix(ATOMIC_WRITE_TMP_PREFIX)
            .tempfile_in(&parent)?;
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
        // Never surface in-flight atomic-write temporaries as objects: a
        // concurrent writer may have just created an empty temp file here.
        if name.starts_with(ATOMIC_WRITE_TMP_PREFIX) {
            continue;
        }
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

#[async_trait]
impl UploadSessionStore for Backend {
    async fn create_upload(&self, key: &str) -> Result<UploadSession, Error> {
        // Create the empty staging file so `write_upload` can re-open it in
        // append mode without needing a separate "first write" path.
        self.put(key, Bytes::new()).await?;
        Ok(UploadSession {
            key: key.to_string(),
            uploaded_size: 0,
            state: SessionState::Fs,
        })
    }

    async fn write_upload(
        &self,
        session: &mut UploadSession,
        _staged_dir: &str,
        body: ByteStream,
        len: u64,
    ) -> Result<(), Error> {
        if !matches!(session.state, SessionState::Fs) {
            return Err(Error::Backend(
                "upload session is not an FS session".to_string(),
            ));
        }
        if len == 0 {
            return Ok(());
        }
        let (mut file, current) = self.open_for_write(&session.key, true).await?;
        if current != session.uploaded_size {
            return Err(Error::Backend(format!(
                "fs upload-session offset drift: file has {current} bytes, session expected {}",
                session.uploaded_size,
            )));
        }
        let mut reader = StreamReader::new(body.map_err(io::Error::other));
        let written = tokio::io::copy(&mut reader, &mut file).await?;
        if written != len {
            return Err(Error::Backend(format!(
                "fs upload-session short body: expected {len} bytes, got {written}",
            )));
        }
        file.flush().await?;
        session.uploaded_size = session
            .uploaded_size
            .checked_add(written)
            .ok_or_else(|| Error::Backend("session size overflow".to_string()))?;
        Ok(())
    }

    async fn complete_upload(
        &self,
        session: UploadSession,
        _staged_dir: &str,
    ) -> Result<(), Error> {
        if !matches!(session.state, SessionState::Fs) {
            return Err(Error::Backend(
                "upload session is not an FS session".to_string(),
            ));
        }
        // No-op: the data already lives at `session.key`. The caller's
        // transactional move (Mutation::Move) promotes it to the canonical
        // location.
        Ok(())
    }

    async fn abort_upload(&self, session: UploadSession, _staged_dir: &str) -> Result<(), Error> {
        if !matches!(session.state, SessionState::Fs) {
            return Err(Error::Backend(
                "upload session is not an FS session".to_string(),
            ));
        }
        self.delete(&session.key).await
    }

    async fn abort_pending_uploads(&self, _key: &str) -> Result<(), Error> {
        // FS sessions have no out-of-band state — the staging file at `key`
        // is the only artifact and `abort_upload` already covers it.
        Ok(())
    }

    // `list_multipart_uploads` / `abort_multipart_upload` use the trait's
    // empty/no-op defaults: FS has no multipart protocol.
}

#[cfg(test)]
mod tests;
