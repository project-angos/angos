//! Filesystem-backed [`ObjectStore`], including the upload-session methods.
//!
//! Maps the storage trait surface onto a directory tree rooted at `root_dir`:
//! every object key becomes a relative path under the root. Writes are atomic
//! (temp-file + rename) and `delete_prefix` walks the subtree, then prunes any
//! ancestor directories it leaves empty, an FS-only concern (S3 has no
//! directories) that stays internal to this backend. Listings sort
//! lexicographically because `read_dir` returns entries in arbitrary order.
//!
//! Does **not** implement [`ConditionalStore`](crate::ConditionalStore): on
//! FS conditional updates are handled one layer up via the metadata store's
//! lock backend.
//!
//! Uploads use an append-mode file at the upload `key`: no staging artifacts,
//! no multipart protocol, no caller-held session. `complete_upload` is a no-op
//! because the data is already at `key`; the caller's transactional move to the
//! canonical location is the only finalisation step.

use std::{
    fs::{File, FileType},
    io::{self, ErrorKind, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use tempfile::Builder as TempFileBuilder;
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
    task::{spawn_blocking, yield_now},
};
use tokio_util::io::StreamReader;

use crate::{
    BoxedReader, ByteStream, ChildrenPage, Error, ObjectMeta, ObjectStore, Page, object::dir_prefix,
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

/// How many times [`ensure_parent`] retries a racing `create_dir_all`. On
/// macOS/APFS a contended intermediate `mkdir` can transiently return EINVAL
/// instead of EEXIST, failing the recursive create even though the directory
/// is about to exist.
const ENSURE_PARENT_RETRIES: u32 = 5;

/// Builder for [`Backend`]. The root directory is required and supplied to
/// [`Backend::builder`]; `sync_to_disk` is an optional fluent setter.
pub struct Builder {
    root: PathBuf,
    sync_to_disk: bool,
}

impl Builder {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            sync_to_disk: false,
        }
    }

    /// When `true`, every write `fsync`s before the temp-file rename. Adds
    /// durability at the cost of a syscall per write.
    #[must_use]
    pub fn sync_to_disk(mut self, sync: bool) -> Self {
        self.sync_to_disk = sync;
        self
    }

    /// Consume the builder and produce the [`Backend`].
    #[must_use]
    pub fn build(self) -> Backend {
        Backend {
            root: self.root,
            sync_to_disk: self.sync_to_disk,
        }
    }
}

/// Filesystem [`ObjectStore`] implementation.
#[derive(Clone, Debug)]
pub struct Backend {
    root: PathBuf,
    sync_to_disk: bool,
}

impl Backend {
    /// Return a builder for a filesystem backend rooted at `root`, under which
    /// all object keys are resolved. Other settings are optional fluent setters
    /// on the returned builder.
    #[must_use]
    pub fn builder(root: impl Into<PathBuf>) -> Builder {
        Builder::new(root.into())
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// Open `key` for writing in append mode (`O_APPEND`), creating the file
    /// and its parent directory if they do not exist.
    ///
    /// Returns the open file handle and the file's size **before** the open,
    /// so the upload can report its new total size without a separate `head`
    /// call.
    ///
    /// Private: this is an upload implementation detail, not part of the
    /// [`ObjectStore`] trait surface.
    ///
    /// # Errors
    /// Returns [`Error::Backend`] when the file cannot be opened.
    async fn open_for_append(&self, key: &str) -> Result<(fs::File, u64), Error> {
        let path = self.full_path(key);
        ensure_parent(&path).await?;
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| backend_error("open", &path, &e))?;
        let size = file.metadata().await?.len();
        Ok((file, size))
    }

    /// Best-effort cleanup of empty ancestor directories after a leaf
    /// deletion. Walks parents upward until it reaches the first non-empty
    /// parent or `root_dir` (exclusive). The store root is the only ceiling.
    /// Errors are suppressed.
    ///
    /// Private: empty-directory pruning is an FS implementation detail with no
    /// meaning on S3, so it never crosses the [`ObjectStore`] trait surface.
    /// [`Backend::delete_prefix`] invokes it for callers; nothing outside this
    /// module triggers it directly.
    async fn prune_empty_ancestors(&self, key: &str) {
        // Lexically resolve `.`/`..` BEFORE touching the filesystem. `full_path`
        // does a bare `root.join(key)`, and `Path::starts_with` is purely
        // lexical: it does not collapse `..`. Without normalization a key like
        // `a/../../x` yields ancestor paths that still contain `..`, slip past a
        // `starts_with(root)` guard, and resolve at syscall time to a directory
        // *above* the configured root, where `remove_dir` could delete it.
        let root = normalize_lexical(&self.root);
        let start = normalize_lexical(&self.full_path(key));
        // Refuse to touch anything that does not lie strictly under the root.
        if !start.starts_with(&root) || start == root {
            return;
        }
        let mut current = start.parent();
        while let Some(parent) = current {
            // The store root is the ceiling: never remove it or anything above.
            // Both paths are normalized, so this comparison is `..`-proof.
            if parent == root || !parent.starts_with(&root) {
                break;
            }
            let Ok(mut entries) = fs::read_dir(parent).await else {
                break;
            };
            // Stop at the first non-empty parent (or any read error).
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

/// Resolve `.` and `..` components purely lexically, with no I/O.
///
/// Unlike [`Path::canonicalize`] this performs no syscalls, follows no
/// symlinks, and does not require the path to exist: it is a pure string-level
/// collapse used to make ancestor pruning escape-proof. `Prefix`/`RootDir`
/// components are preserved, `.` is dropped, `..` pops the last `Normal`
/// component (but never the leading prefix/root), and `Normal` components are
/// pushed. A `..` that would climb above the prefix/root is simply ignored, so
/// the result can never lexically escape the path's own anchor.
fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
            // Pop only a real path segment; never climb past the prefix/root anchor.
            Component::ParentDir => {
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                }
            }
            Component::Normal(part) => out.push(part),
        }
    }
    out
}

/// Create all parent directories of `path`.
async fn ensure_parent(path: &Path) -> Result<(), Error> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    // Retry only race error kinds: EINVAL (macOS/APFS, see
    // ENSURE_PARENT_RETRIES), EEXIST, and ENOENT from a racing remove; other
    // errors are deterministic and surface immediately.
    let mut attempt = 0;
    loop {
        match fs::create_dir_all(parent).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if fs::metadata(parent).await.is_ok_and(|m| m.is_dir()) {
                    return Ok(());
                }
                let racy = matches!(
                    e.kind(),
                    ErrorKind::InvalidInput | ErrorKind::AlreadyExists | ErrorKind::NotFound
                );
                attempt += 1;
                if !racy || attempt >= ENSURE_PARENT_RETRIES {
                    return Err(backend_error("create_dir_all", parent, &e));
                }
                yield_now().await;
            }
        }
    }
}

/// Map an io error to an [`Error`] carrying the operation and path for
/// diagnosability. Preserves the `NotFound` classification.
fn backend_error(op: &str, path: &Path, error: &io::Error) -> Error {
    if error.kind() == ErrorKind::NotFound {
        Error::NotFound
    } else {
        Error::Backend(format!("{op} {}: {error}", path.display()))
    }
}

async fn atomic_write(target: &Path, data: Bytes, sync: bool) -> Result<(), Error> {
    ensure_parent(target).await?;
    let parent = target.parent().unwrap_or_else(|| Path::new(".")).to_owned();
    let final_path = target.to_owned();
    let label = target.to_owned();
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
    .map_err(|e| Error::Backend(format!("temp-write task panicked: {e}")))?
    .map_err(|e| backend_error("atomic_write", &label, &e))?;
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
        // An empty prefix is a no-op (see `dir_prefix`): it must not be
        // resolved to the store root and recursively wipe every object.
        if dir_prefix(prefix).is_empty() {
            return Ok(());
        }
        let path = self.full_path(prefix);
        // Directory → recursive remove; file → unlink; missing → success.
        match fs::metadata(&path).await {
            Ok(meta) if meta.is_dir() => match fs::remove_dir_all(&path).await {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            },
            Ok(_) => self.delete(prefix).await?,
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        // Removing the subtree may have emptied its parent shard directories;
        // tidy them up to the store root so deletions don't leave hollow
        // scaffolding behind. This is an FS-only concern, kept internal to the
        // backend.
        self.prune_empty_ancestors(prefix).await;
        Ok(())
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
        let src = self.full_path(source);
        let dst = self.full_path(destination);
        ensure_parent(&dst).await?;
        let parent = dst.parent().unwrap_or_else(|| Path::new(".")).to_owned();
        let sync = self.sync_to_disk;
        // Stream src -> temp -> atomic rename. `std::io::copy` uses a small
        // internal buffer, so the object body is never held in memory in full
        // (a multi-GB blob would otherwise spike RSS by its whole size).
        match spawn_blocking(move || -> Result<(), Error> {
            let mut reader = File::open(&src).map_err(|e| backend_error("copy from", &src, &e))?;
            let mut temp = TempFileBuilder::new()
                .prefix(ATOMIC_WRITE_TMP_PREFIX)
                .tempfile_in(&parent)
                .map_err(|e| backend_error("copy to", &dst, &e))?;
            io::copy(&mut reader, temp.as_file_mut())
                .map_err(|e| backend_error("copy to", &dst, &e))?;
            if sync {
                temp.as_file()
                    .sync_all()
                    .map_err(|e| backend_error("copy to", &dst, &e))?;
            }
            temp.persist(&dst)
                .map_err(|e| backend_error("copy to", &dst, &e.error))?;
            Ok(())
        })
        .await
        {
            Ok(result) => result,
            Err(e) => Err(Error::Backend(format!("copy task panicked: {e}"))),
        }
    }

    async fn move_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        let src = self.full_path(source);
        let dst = self.full_path(destination);
        ensure_parent(&dst).await?;
        // Same-filesystem rename is atomic and O(1) in memory: the staging
        // upload and its canonical blob-data location live under the same root,
        // so this is the common path and avoids copying the bytes at all.
        if fs::rename(&src, &dst).await.is_ok() {
            return Ok(());
        }
        // Rename can fail across filesystems (`EXDEV`) or transiently; fall back
        // to the (streamed) copy + delete that the trait default would do.
        self.copy(source, destination).await?;
        self.delete(source).await
    }

    async fn create_upload(&self, key: &str) -> Result<(), Error> {
        // Create/truncate the staging file at `key` so a re-`create` at a reused
        // key starts from an empty file and `write_upload` can re-open it in
        // append mode.
        self.put(key, Bytes::new()).await
    }

    async fn write_upload(&self, key: &str, body: ByteStream, len: u64) -> Result<u64, Error> {
        let (mut file, current) = self.open_for_append(key).await?;
        if len == 0 {
            return Ok(current);
        }
        let mut reader = StreamReader::new(body.map_err(io::Error::other));
        let written = tokio::io::copy(&mut reader, &mut file).await?;
        if written != len {
            return Err(Error::Backend(format!(
                "fs upload short body: expected {len} bytes, got {written}",
            )));
        }
        file.flush().await?;
        current
            .checked_add(written)
            .ok_or_else(|| Error::Backend("upload size overflow".to_string()))
    }

    async fn complete_upload(&self, key: &str) -> Result<(), Error> {
        // Ensure the staging file exists (an upload completed with no writes
        // still produces an empty object at `key`), then no-op: the data already
        // lives at `key`. The caller's transactional move (Mutation::Move)
        // promotes it to the canonical location.
        match self.head(key).await {
            Ok(_) => Ok(()),
            Err(Error::NotFound) => self.put(key, Bytes::new()).await,
            Err(e) => Err(e),
        }
    }

    async fn abort_upload(&self, key: &str) -> Result<(), Error> {
        // The staging file at `key` is the only artifact; deleting it is
        // idempotent (missing file counts as success).
        self.delete(key).await
    }

    // `list_multipart_uploads` uses the trait's empty default: FS has no
    // multipart protocol.
}

#[cfg(test)]
mod tests;
