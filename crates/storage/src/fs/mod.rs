//! Filesystem [`ObjectStore`]: each key is a path under the root, writes are
//! temp-file plus rename, and deletes prune the directories they empty, as S3
//! has none. An upload is an append-mode file at its key.

use std::fs::{File, FileType, hard_link};
use std::io::{self, ErrorKind, Read, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::try_unfold;
use futures_util::{Stream, StreamExt, TryStreamExt};
use tempfile::{Builder as TempFileBuilder, NamedTempFile};
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, copy as async_copy};
use tokio::task::{spawn_blocking, yield_now};
use tokio_util::io::StreamReader;

use crate::object::verified_move;
use crate::{
    BoxedReader, ByteStream, Children, ChildrenPage, Error, KeyStream, ObjectMeta, ObjectStore,
    Page,
};

/// Prefix of in-flight write temporaries, which listings skip: a reader would
/// otherwise see a still-empty object.
const ATOMIC_WRITE_TMP_PREFIX: &str = ".angos-write.";

/// Retries of a racing `create_dir_all`: on macOS/APFS a contended `mkdir` can
/// return EINVAL instead of EEXIST.
const ENSURE_PARENT_RETRIES: u32 = 5;

#[derive(Clone, Debug)]
pub struct Backend {
    root: PathBuf,
    sync_to_disk: bool,
}

impl Backend {
    /// With `sync_to_disk`, every write is `fsync`ed before it becomes visible.
    #[must_use]
    pub fn new(root: &Path, sync_to_disk: bool) -> Self {
        Self {
            root: root.to_path_buf(),
            sync_to_disk,
        }
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// Best-effort removal of the empty ancestors of `key`, up to but never
    /// including the root.
    async fn prune_empty_ancestors(&self, key: &str) {
        // `starts_with` is lexical, so an unnormalized `a/../../x` would pass
        // the root guard and resolve above the root at syscall time.
        let root = normalize_lexical(&self.root);
        let start = normalize_lexical(&self.full_path(key));
        if !start.starts_with(&root) || start == root {
            return;
        }
        let mut current = start.parent();
        while let Some(parent) = current {
            if parent == root || !parent.starts_with(&root) {
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
}

/// Resolves `.` and `..` without syscalls; a `..` never climbs past the anchor.
fn normalize_lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => out.push(component.as_os_str()),
            Component::CurDir => {}
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

async fn ensure_parent(path: &Path) -> Result<(), Error> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    // Retry only the race kinds: EINVAL (APFS), EEXIST, and ENOENT from a
    // racing prune.
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

fn backend_error(op: &str, path: &Path, error: &io::Error) -> Error {
    if error.kind() == ErrorKind::NotFound {
        Error::NotFound
    } else {
        Error::Backend(format!("{op} {}: {error}", path.display()))
    }
}

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

/// Streams `src` into a temp file renamed onto `dst`. `exact` copies that many
/// bytes and refuses to publish fewer.
async fn copy_into_place(
    src: PathBuf,
    dst: PathBuf,
    sync: bool,
    exact: Option<u64>,
) -> Result<u64, Error> {
    ensure_parent(&dst).await?;
    let parent = dst.parent().unwrap_or_else(|| Path::new(".")).to_owned();
    spawn_blocking(move || -> Result<u64, Error> {
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
    .map_err(|e| Error::Backend(format!("copy task panicked: {e}")))?
}

/// Runs `write` on a blocking thread, retrying when a concurrent prune removes
/// the parent before the temp file lands in it.
async fn write_beside<T, F>(target: &Path, op: &str, write: F) -> Result<T, Error>
where
    T: Send + 'static,
    F: Fn(&Path) -> io::Result<T> + Clone + Send + 'static,
{
    let mut attempt = 0;
    loop {
        ensure_parent(target).await?;
        let parent = target.parent().unwrap_or_else(|| Path::new(".")).to_owned();
        let write = write.clone();
        let result = spawn_blocking(move || write(&parent))
            .await
            .map_err(|e| Error::Backend(format!("{op} task panicked: {e}")))?;
        match result {
            Ok(value) => return Ok(value),
            Err(e) => {
                let racy = matches!(e.kind(), ErrorKind::InvalidInput | ErrorKind::NotFound);
                attempt += 1;
                if !racy || attempt >= ENSURE_PARENT_RETRIES {
                    return Err(backend_error(op, target, &e));
                }
                yield_now().await;
            }
        }
    }
}

fn partition_entries(entries: impl IntoIterator<Item = (String, FileType)>) -> Children {
    let (dirs, files): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|(_, file_type)| file_type.is_dir());
    let names =
        |entries: Vec<(String, FileType)>| entries.into_iter().map(|(name, _)| name).collect();
    Children {
        sub_prefixes: names(dirs),
        objects: names(files),
    }
}

/// Missing directory yields no entries.
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
        if name.starts_with(ATOMIC_WRITE_TMP_PREFIX) {
            continue;
        }
        let file_type = entry.file_type().await?;
        entries.push((name, file_type));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(entries)
}

/// Every key under `root` strictly above `after`, in S3's lexical order: a
/// directory sorts as its name plus `/` (`a.txt` < `a/x` < `a0`), so one at or
/// below `after` is never read. Holds only the pending siblings along one path.
fn walk_sorted(
    root: PathBuf,
    after: Option<String>,
) -> impl Stream<Item = Result<String, Error>> + Send {
    // Each pending entry is a key, or a directory's prefix (`true`) ending in
    // `/`, the root's being empty; the last one sorts first.
    let pending = vec![(String::new(), true)];
    try_unfold(
        (pending, root, after),
        |(mut pending, root, after)| async move {
            while let Some((key, is_dir)) = pending.pop() {
                if !is_dir {
                    return Ok(Some((key, (pending, root, after))));
                }
                let mut children: Vec<(String, bool)> = read_dir_sorted(&root.join(&key))
                    .await?
                    .into_iter()
                    .map(|(name, file_type)| {
                        if file_type.is_dir() {
                            (format!("{key}{name}/"), true)
                        } else {
                            (format!("{key}{name}"), false)
                        }
                    })
                    // A directory the cursor lies in still holds keys above it.
                    .filter(|(child, is_dir)| {
                        after.as_deref().is_none_or(|after| {
                            child.as_str() > after || (*is_dir && after.starts_with(child.as_str()))
                        })
                    })
                    .collect();
                children.sort();
                pending.extend(children.into_iter().rev());
            }
            Ok(None)
        },
    )
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
        let target = self.full_path(key);
        let final_path = target.clone();
        let sync = self.sync_to_disk;
        write_beside(&target, "atomic_write", move |parent| {
            staged_file(parent, sync, |file| file.write_all(&data))?
                .persist(&final_path)
                .map_err(|e| e.error)?;
            Ok(())
        })
        .await
    }

    async fn create_if_absent(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        let target = self.full_path(key);
        let link = target.clone();
        let sync = self.sync_to_disk;
        // `link(2)` is the atomic create-if-absent that also holds on NFS: the
        // temp file is fully written before the name appears.
        write_beside(&target, "create_if_absent", move |parent| {
            let temp = staged_file(parent, sync, |file| file.write_all(&data))?;
            match hard_link(temp.path(), &link) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
                Err(e) => Err(e),
            }
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        match fs::remove_file(self.full_path(key)).await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        // Else an empty tag directory would list as a live tag.
        self.prune_empty_ancestors(key).await;
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), Error> {
        if prefix.is_empty() {
            return Ok(());
        }
        let path = self.full_path(prefix);
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
        let n = usize::from(n);
        // The one key past the page only tells whether more remain.
        let mut items: Vec<String> = walk_sorted(self.full_path(prefix), token.or(start_after))
            .take(n + 1)
            .try_collect()
            .await?;
        let more = items.len() > n;
        items.truncate(n);
        let next_token = more.then(|| items.last().cloned()).flatten();
        Ok(Page { items, next_token })
    }

    /// One walk, rather than re-reading each page's cursor path.
    fn list_in_order<'a>(&'a self, prefix: &'a str) -> KeyStream<'a> {
        Box::pin(walk_sorted(self.full_path(prefix), None))
    }

    async fn list_children(
        &self,
        prefix: &str,
        n: u16,
        token: Option<String>,
        start_after: Option<String>,
    ) -> Result<ChildrenPage, Error> {
        let entries = read_dir_sorted(&self.full_path(prefix)).await?;

        let cursor = token.as_deref().or(start_after.as_deref());
        let lower = cursor.map_or(0, |t| {
            entries
                .iter()
                .position(|(name, _)| name.as_str() > t)
                .unwrap_or(entries.len())
        });
        let upper = (lower + n as usize).min(entries.len());
        let slice = &entries[lower..upper];

        let Children {
            sub_prefixes,
            objects,
        } = partition_entries(slice.iter().cloned());

        let next_token = (upper < entries.len())
            .then(|| slice.last().map(|(name, _)| name.clone()))
            .flatten();

        Ok(ChildrenPage {
            sub_prefixes,
            objects,
            next_token,
        })
    }

    async fn list_all_children(&self, prefix: &str) -> Result<Children, Error> {
        let entries = read_dir_sorted(&self.full_path(prefix)).await?;
        Ok(partition_entries(entries))
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
        if fs::rename(&src, &dst).await.is_ok() {
            self.prune_empty_ancestors(source).await;
            return Ok(());
        }
        // A rename across filesystems (`EXDEV`) falls back to copy and delete.
        verified_move(self, source, destination).await
    }

    async fn write_upload(
        &self,
        key: &str,
        body: ByteStream,
        len: Option<u64>,
    ) -> Result<u64, Error> {
        let path = self.full_path(key);
        ensure_parent(&path).await?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| backend_error("open", &path, &e))?;
        let current = file.metadata().await?.len();
        if len == Some(0) {
            return Ok(current);
        }
        let mut reader = StreamReader::new(body.map_err(io::Error::other));
        let written = async_copy(&mut reader, &mut file).await?;
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
        // The data is already at `key`; only an upload with no writes needs
        // its empty object created.
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
        self.delete(key).await
    }
}

#[cfg(test)]
mod tests;
