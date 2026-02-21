use std::{
    io::{ErrorKind, Write},
    path::PathBuf,
};

use serde::Deserialize;
use tokio::fs;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct BackendConfig {
    pub root_dir: String,
    #[serde(default)]
    pub sync_to_disk: bool,
}

#[derive(Clone, Debug)]
pub struct Backend {
    root: PathBuf,
    sync_to_disk: bool,
}

impl Backend {
    pub fn new(config: &BackendConfig) -> Self {
        Self {
            root: PathBuf::from(&config.root_dir),
            sync_to_disk: config.sync_to_disk,
        }
    }

    fn full_path(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }

    pub async fn read(&self, path: &str) -> Result<Vec<u8>, std::io::Error> {
        fs::read(self.full_path(path)).await
    }

    pub async fn read_to_string(&self, path: &str) -> Result<String, std::io::Error> {
        fs::read_to_string(self.full_path(path)).await
    }

    pub async fn write(&self, path: &str, data: &[u8]) -> Result<(), std::io::Error> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut temp_file = tempfile::NamedTempFile::new_in(
            full_path.parent().unwrap_or(std::path::Path::new(".")),
        )?;

        temp_file.write_all(data)?;

        if self.sync_to_disk {
            temp_file.flush()?;
            temp_file.as_file().sync_all()?;
        }

        temp_file.persist(full_path)?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn delete(&self, path: &str) -> Result<(), std::io::Error> {
        let full_path = self.full_path(path);
        match fs::remove_file(&full_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn delete_dir(&self, path: &str) -> Result<(), std::io::Error> {
        let full_path = self.full_path(path);
        match fs::remove_dir_all(&full_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn file_size(&self, path: &str) -> Result<u64, std::io::Error> {
        let metadata = fs::metadata(self.full_path(path)).await?;
        Ok(metadata.len())
    }

    pub async fn list_dir(&self, path: &str) -> Result<Vec<String>, std::io::Error> {
        let full_path = self.full_path(path);
        let mut entries = Vec::new();

        let mut read_dir = match fs::read_dir(full_path).await {
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

    pub async fn delete_empty_parent_dirs(&self, path: &str) -> Result<(), std::io::Error> {
        let full_path = self.full_path(path);
        let mut current_path = full_path.parent();

        while let Some(parent_path) = current_path {
            if parent_path == self.root || !parent_path.starts_with(&self.root) {
                break;
            }

            let Ok(mut entries) = fs::read_dir(parent_path).await else {
                break;
            };

            if entries.next_entry().await?.is_some() {
                break;
            }

            fs::remove_dir(parent_path).await?;
            current_path = parent_path.parent();
        }

        Ok(())
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<(), std::io::Error> {
        let from_path = self.full_path(from);
        let to_path = self.full_path(to);

        if let Some(parent) = to_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        fs::rename(from_path, to_path).await
    }

    pub async fn open_file(&self, path: &str) -> Result<fs::File, std::io::Error> {
        fs::File::open(self.full_path(path)).await
    }

    pub async fn create_file(&self, path: &str) -> Result<fs::File, std::io::Error> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::File::create(full_path).await
    }

    pub async fn open_file_append(&self, path: &str) -> Result<fs::File, std::io::Error> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(full_path)
            .await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_new() {
        let config = BackendConfig {
            root_dir: "/test/path".to_string(),
            sync_to_disk: true,
        };
        let backend = Backend::new(&config);
        assert_eq!(backend.root, PathBuf::from("/test/path"));
        assert!(backend.sync_to_disk);
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("test.txt", b"hello world").await.unwrap();
        let content = backend.read("test.txt").await.unwrap();
        assert_eq!(content, b"hello world");
    }

    #[tokio::test]
    async fn test_write_with_sync_to_disk() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: true,
        };
        let backend = Backend::new(&config);

        backend.write("test.txt", b"synced").await.unwrap();
        let content = backend.read("test.txt").await.unwrap();
        assert_eq!(content, b"synced");
    }

    #[tokio::test]
    async fn test_read_to_string() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("test.txt", b"hello string").await.unwrap();
        let content = backend.read_to_string("test.txt").await.unwrap();
        assert_eq!(content, "hello string");
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("test.txt", b"delete me").await.unwrap();
        backend.delete("test.txt").await.unwrap();
        assert!(backend.read("test.txt").await.is_err());

        backend.delete("non_existent.txt").await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_dir() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("dir/file1.txt", b"content1").await.unwrap();
        backend.write("dir/file2.txt", b"content2").await.unwrap();

        backend.delete_dir("dir").await.unwrap();
        assert!(backend.read("dir/file1.txt").await.is_err());

        backend.delete_dir("non_existent_dir").await.unwrap();
    }

    #[tokio::test]
    async fn test_file_size() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        let data = b"test data";
        backend.write("test.txt", data).await.unwrap();
        let size = backend.file_size("test.txt").await.unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[tokio::test]
    async fn test_list_dir() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("dir/file1.txt", b"content1").await.unwrap();
        backend.write("dir/file2.txt", b"content2").await.unwrap();

        let mut entries = backend.list_dir("dir").await.unwrap();
        entries.sort();
        assert_eq!(entries, vec!["file1.txt", "file2.txt"]);

        let empty = backend.list_dir("non_existent").await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_delete_empty_parent_dirs() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("a/b/c/file.txt", b"content").await.unwrap();
        backend.delete("a/b/c/file.txt").await.unwrap();

        backend
            .delete_empty_parent_dirs("a/b/c/file.txt")
            .await
            .unwrap();

        assert!(backend.list_dir("a/b/c").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_rename() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("old.txt", b"content").await.unwrap();
        backend.rename("old.txt", "new.txt").await.unwrap();

        assert!(backend.read("old.txt").await.is_err());
        let content = backend.read("new.txt").await.unwrap();
        assert_eq!(content, b"content");
    }

    #[tokio::test]
    async fn test_open_file() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("test.txt", b"content").await.unwrap();
        let file = backend.open_file("test.txt").await.unwrap();
        assert!(file.metadata().await.unwrap().is_file());
    }

    #[tokio::test]
    async fn test_create_file() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        let mut file = backend.create_file("new.txt").await.unwrap();
        file.write_all(b"new content").await.unwrap();
        drop(file);

        let content = backend.read("new.txt").await.unwrap();
        assert_eq!(content, b"new content");
    }

    #[tokio::test]
    async fn test_open_file_append() {
        let tmp_dir = TempDir::new().unwrap();
        let config = BackendConfig {
            root_dir: tmp_dir.path().to_string_lossy().into_owned(),
            sync_to_disk: false,
        };
        let backend = Backend::new(&config);

        backend.write("test.txt", b"initial").await.unwrap();

        let mut file = backend.open_file_append("test.txt").await.unwrap();
        file.write_all(b" appended").await.unwrap();
        drop(file);

        let content = backend.read("test.txt").await.unwrap();
        assert_eq!(content, b"initial appended");
    }
}
