//! Compound async filesystem helpers shared between the `fs` blob and
//! metadata stores. These wrap `tokio::fs` primitives with the small
//! additions both consumers need (atomic write-via-rename, recursive empty
//! parent cleanup, list/delete with `NotFound` normalised to success). All
//! callers hold their own `root: PathBuf` and pre-compute absolute paths;
//! these functions are pure helpers with no state of their own.

use std::{
    io::{self, ErrorKind, Write as _},
    path::Path,
};

use tempfile::NamedTempFile;
use tokio::{fs, task::spawn_blocking};

/// Writes `data` to `path` atomically: stages it in a sibling temp file and
/// renames into place. `sync_to_disk = true` adds an `fsync` before persist.
pub async fn atomic_write(path: &Path, data: &[u8], sync_to_disk: bool) -> io::Result<()> {
    ensure_parent_dir(path).await?;
    let parent = path.parent().unwrap_or_else(|| Path::new(".")).to_owned();
    let data = data.to_vec();
    let final_path = path.to_owned();
    spawn_blocking(move || -> io::Result<()> {
        let mut temp_file = NamedTempFile::new_in(&parent)?;
        temp_file.write_all(&data)?;
        if sync_to_disk {
            temp_file.flush()?;
            temp_file.as_file().sync_all()?;
        }
        temp_file.persist(final_path).map_err(|e| e.error)?;
        Ok(())
    })
    .await
    .map_err(|e| io::Error::other(e.to_string()))?
}

/// Best-effort cleanup of empty ancestor directories after a leaf deletion.
///
/// Walks **at most `max_levels`** parents upward from `start`, removing each
/// one only while it is empty. The walk stops at the first non-empty parent,
/// at `root` (exclusive), or as soon as it would leave the `root` subtree.
/// Errors short-circuit the loop instead of bubbling up — this is a best-
/// effort tidy-up, not a load-bearing step.
///
/// The bounded depth and the strict `starts_with(root)` guard exist to make
/// this safe to call with any path that originated inside `root`: even with a
/// pathological `start` we cannot walk above the bucket or run away.
pub async fn prune_empty_ancestors(start: &Path, root: &Path, max_levels: u8) -> io::Result<()> {
    let mut current = start.parent();
    for _ in 0..max_levels {
        let Some(parent) = current else { break };
        if parent == root || !parent.starts_with(root) {
            break;
        }
        let Ok(mut entries) = fs::read_dir(parent).await else {
            break;
        };
        if entries.next_entry().await?.is_some() {
            break;
        }
        // Race-safe: if another writer beat us to recreating an entry,
        // `remove_dir` will fail with ENOTEMPTY — treat that as a stop signal.
        if fs::remove_dir(parent).await.is_err() {
            break;
        }
        current = parent.parent();
    }
    Ok(())
}

/// Lists the immediate entry names of `path`. A missing directory yields an
/// empty vector instead of `NotFound` — callers rely on this.
pub async fn list_dir_or_empty(path: &Path) -> io::Result<Vec<String>> {
    let mut entries = Vec::new();
    let mut read_dir = match fs::read_dir(path).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(entries),
        Err(e) => return Err(e),
    };
    while let Some(entry) = read_dir.next_entry().await? {
        if let Some(name) = entry.file_name().to_str() {
            entries.push(name.to_string());
        }
    }
    Ok(entries)
}

/// Recursively removes `path`. A missing path counts as success.
pub async fn remove_dir_all_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Removes `path` (a regular file). A missing path counts as success.
pub async fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    Ok(())
}
