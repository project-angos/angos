//! Filesystem-backed [`ObjectStore`], including the upload-session methods.
//!
//! Maps the storage trait surface onto a directory tree rooted at `root_dir`:
//! every object key becomes a relative path under the root. Writes are atomic
//! (temp-file + rename) and `delete_prefix` walks the subtree, then prunes any
//! ancestor directories it leaves empty, an FS-only concern (S3 has no
//! directories) that stays internal to this backend. Listings sort
//! lexicographically because `read_dir` returns entries in arbitrary order.
//!
//! Uploads use an append-mode file at the upload `key`: no staging artifacts,
//! no multipart protocol, no caller-held session. `complete_upload` is a no-op
//! because the data is already at `key`; `promote_upload` copies the verified
//! bytes to the canonical location, since an append that is still in flight
//! keeps writing to the file it opened.

use std::{
    collections::VecDeque,
    fs::{File, FileType},
    io::{self, ErrorKind, Read, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt, stream::try_unfold};
use tempfile::{Builder as TempFileBuilder, NamedTempFile};
use tokio::{
    fs,
    io::{AsyncSeekExt, AsyncWriteExt},
    task::{spawn_blocking, yield_now},
};
use tokio_util::io::StreamReader;

use crate::{
    BoxedReader, ByteStream, Children, ChildrenPage, Error, KeyStream, ObjectMeta, ObjectStore,
    Page,
    object::{dir_prefix, verified_move},
};

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
    /// [`Backend::delete`] and [`Backend::delete_prefix`] invoke it for
    /// callers; nothing outside this module triggers it directly.
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

/// A temp file beside the objects of `parent`, filled by `fill` and synced
/// when asked, ready to be persisted or linked into place.
fn staged_file(
    parent: &Path,
    sync: bool,
    fill: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<NamedTempFile> {
    let mut temp = TempFileBuilder::new()
        .prefix(ATOMIC_WRITE_TMP_PREFIX)
        .tempfile_in(parent)?;
    fill(temp.as_file_mut())?;
    if sync {
        temp.as_file().sync_all()?;
    }
    Ok(temp)
}

/// Stream `src` into a temp file and rename it onto `dst`; `std::io::copy`
/// buffers little, so a multi-GB blob never sits in memory. `exact` copies
/// that many bytes and refuses to publish fewer.
async fn copy_into_place(
    src: PathBuf,
    dst: PathBuf,
    sync: bool,
    exact: Option<u64>,
) -> Result<u64, Error> {
    ensure_parent(&dst).await?;
    let parent = dst.parent().unwrap_or_else(|| Path::new(".")).to_owned();
    match spawn_blocking(move || -> Result<u64, Error> {
        let reader = File::open(&src).map_err(|e| backend_error("copy from", &src, &e))?;
        let mut copied = 0;
        let temp = staged_file(&parent, sync, |file| {
            copied = io::copy(&mut reader.take(exact.unwrap_or(u64::MAX)), file)?;
            Ok(())
        })
        .map_err(|e| backend_error("copy to", &dst, &e))?;
        if let Some(size) = exact
            && copied != size
        {
            return Err(Error::Backend(format!(
                "copy of {} ended at {copied} bytes, expected {size}",
                src.display()
            )));
        }
        temp.persist(&dst)
            .map_err(|e| backend_error("copy to", &dst, &e.error))?;
        Ok(copied)
    })
    .await
    {
        Ok(result) => result,
        Err(e) => Err(Error::Backend(format!("copy task panicked: {e}"))),
    }
}

async fn atomic_write(target: &Path, data: Bytes, sync: bool) -> Result<(), Error> {
    // A concurrent `delete` can prune the parent directory between
    // `ensure_parent` and temp-file creation (see `prune_empty_ancestors`).
    // Once the temp file exists the directory is non-empty and safe from the
    // sweep, so only creation races: retry the racy error kinds with the same
    // bound as `ensure_parent`.
    let mut attempt = 0;
    loop {
        ensure_parent(target).await?;
        let parent = target.parent().unwrap_or_else(|| Path::new(".")).to_owned();
        let final_path = target.to_owned();
        let body = data.clone();
        let result = spawn_blocking(move || -> io::Result<()> {
            staged_file(&parent, sync, |file| file.write_all(&body))?
                .persist(final_path)
                .map_err(|e| e.error)?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Backend(format!("temp-write task panicked: {e}")))?;
        match result {
            Ok(()) => return Ok(()),
            Err(e) => {
                let racy = matches!(e.kind(), ErrorKind::InvalidInput | ErrorKind::NotFound);
                attempt += 1;
                if !racy || attempt >= ENSURE_PARENT_RETRIES {
                    return Err(backend_error("atomic_write", target, &e));
                }
                yield_now().await;
            }
        }
    }
}

/// Split directory entries into `(sub_prefixes, objects)` by file type.
fn partition_entries(
    entries: impl IntoIterator<Item = (String, FileType)>,
) -> (Vec<String>, Vec<String>) {
    let (dirs, files): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|(_, file_type)| file_type.is_dir());
    let names =
        |entries: Vec<(String, FileType)>| entries.into_iter().map(|(name, _)| name).collect();
    (names(dirs), names(files))
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

/// Walk `root` once, streaming every regular-file key relative to `root`
/// itself. Only the directory stack and one directory's entries are held, so a
/// whole-store scan does not materialise the tree.
fn walk_keys(root: PathBuf) -> impl Stream<Item = Result<String, Error>> + Send {
    let stack = vec![root.clone()];
    try_unfold(
        (stack, VecDeque::new(), root),
        |(mut stack, mut ready, root)| async move {
            loop {
                if let Some(key) = ready.pop_front() {
                    return Ok(Some((key, (stack, ready, root))));
                }
                let Some(current) = stack.pop() else {
                    return Ok(None);
                };
                // Reverse so `pop` takes sub-directories, and `push_front`
                // takes files, in name order.
                for (name, file_type) in read_dir_sorted(&current).await?.into_iter().rev() {
                    let child = current.join(&name);
                    if file_type.is_dir() {
                        stack.push(child);
                    } else if let Ok(rel) = child.strip_prefix(&root)
                        && let Some(rel) = rel.to_str()
                    {
                        ready.push_front(rel.to_string());
                    }
                }
            }
        },
    )
}

/// Every regular-file key under `dir`, relative to `dir` itself and sorted
/// lexicographically. Matches the S3 backend, which returns prefix-relative
/// names from `ListObjectsV2`. Depth-first name order is not lexicographic
/// (`a.txt` sorts below `a/x`), so the walk's output is sorted here.
async fn collect_flat_keys(dir: &Path) -> Result<Vec<String>, Error> {
    let mut keys: Vec<String> = walk_keys(dir.to_path_buf()).try_collect().await?;
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

    async fn create_if_absent(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        let target = self.full_path(key);
        ensure_parent(&target).await?;
        let parent = target.parent().unwrap_or_else(|| Path::new(".")).to_owned();
        let sync = self.sync_to_disk;
        spawn_blocking(move || -> Result<bool, Error> {
            // `link(2)` is the atomic create-if-absent that also holds on
            // NFS: the temp file is fully written before the name appears.
            let temp = staged_file(&parent, sync, |file| file.write_all(&data))
                .map_err(|e| backend_error("create_if_absent", &target, &e))?;
            match std::fs::hard_link(temp.path(), &target) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(backend_error("create_if_absent", &target, &e)),
            }
        })
        .await
        .map_err(|e| Error::Backend(format!("create-if-absent task panicked: {e}")))?
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        match fs::remove_file(self.full_path(key)).await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        // On S3 a prefix vanishes with its last object; mirror that here so
        // directory-based listings never surface hollow scaffolding (e.g. a
        // deleted tag's empty directory read back as a live tag).
        self.prune_empty_ancestors(key).await;
        Ok(())
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
            last_modified,
        })
    }

    async fn list_after(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<Page<String>, Error> {
        let all_keys = collect_flat_keys(&self.full_path(prefix)).await?;
        // Listings sort, so a token and a start-after bound resume the same
        // way: skip to the first key strictly above the cursor.
        let cursor = token.or(start_after);
        let start = cursor.as_deref().map_or(0, |t| {
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

    fn list_all<'a>(&'a self, prefix: &'a str) -> KeyStream<'a> {
        // The paged listing re-walks the subtree for every page, which a
        // whole-store scan cannot afford.
        Box::pin(walk_keys(self.full_path(prefix)))
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

        let (sub_prefixes, objects) = partition_entries(slice.iter().cloned());

        let next_token = (upper < entries.len())
            .then(|| slice.last().map(|(name, _)| name.clone()))
            .flatten();

        Ok(ChildrenPage {
            sub_prefixes,
            objects,
            next_token,
        })
    }

    /// One directory read instead of the default page drain, which re-reads
    /// the directory once per page.
    async fn list_all_children(&self, prefix: &str) -> Result<Children, Error> {
        let entries = read_dir_sorted(&self.full_path(prefix)).await?;
        let (sub_prefixes, objects) = partition_entries(entries);
        Ok(Children {
            sub_prefixes,
            objects,
        })
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<u64, Error> {
        copy_into_place(
            self.full_path(source),
            self.full_path(destination),
            self.sync_to_disk,
            None,
        )
        .await
    }

    async fn move_object(&self, source: &str, destination: &str) -> Result<(), Error> {
        let src = self.full_path(source);
        let dst = self.full_path(destination);
        ensure_parent(&dst).await?;
        // Same-filesystem rename is atomic and O(1) in memory: the staging
        // upload and its canonical blob-data location live under the same root,
        // so this is the common path and avoids copying the bytes at all.
        if fs::rename(&src, &dst).await.is_ok() {
            // The rename may have emptied the source's parent chain; sweep it
            // like `delete` does so no hollow directories survive the move.
            self.prune_empty_ancestors(source).await;
            return Ok(());
        }
        // Rename can fail across filesystems (`EXDEV`) or transiently; fall back
        // to the (streamed) copy + delete that the trait default would do.
        verified_move(self, source, destination).await
    }

    async fn create_upload(&self, key: &str) -> Result<(), Error> {
        // Create/truncate the staging file at `key` so a re-`create` at a reused
        // key starts from an empty file and `write_upload` can re-open it in
        // append mode.
        self.put(key, Bytes::new()).await
    }

    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, Error> {
        let (mut file, current) = self.open_for_append(key).await?;
        if len == Some(0) {
            return Ok(current);
        }
        let mut reader = StreamReader::new(body.map_err(io::Error::other));
        let written = tokio::io::copy(&mut reader, &mut file).await?;
        if let Some(expected) = len
            && written != expected
        {
            return Err(Error::Backend(format!(
                "fs upload short body: expected {expected} bytes, got {written}",
            )));
        }
        file.flush().await?;
        // The bytes are the object: a manifest committed over an upload the
        // page cache had not written back would name a truncated blob.
        if self.sync_to_disk {
            file.sync_all().await?;
        }
        current
            .checked_add(written)
            .ok_or_else(|| Error::Backend("upload size overflow".to_string()))
    }

    async fn complete_upload(&self, key: &str) -> Result<(), Error> {
        // Ensure the staging file exists (an upload completed with no writes
        // still produces an empty object at `key`), then no-op: the data already
        // lives at `key` until `promote_upload` publishes it.
        match self.head(key).await {
            Ok(_) => Ok(()),
            Err(Error::NotFound) => self.put(key, Bytes::new()).await,
            Err(e) => Err(e),
        }
    }

    async fn promote_upload(&self, key: &str, destination: &str, size: u64) -> Result<(), Error> {
        // Never a rename: a write that opened the staging file before the
        // promotion would keep appending to the published blob through it.
        copy_into_place(
            self.full_path(key),
            self.full_path(destination),
            self.sync_to_disk,
            Some(size),
        )
        .await?;
        self.delete(key).await
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
