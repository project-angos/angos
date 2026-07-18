use std::collections::BTreeMap;
use std::env;
use std::fs::write;
use std::path::Path;
use std::sync::Arc;

use angos_s3_client::{Backend as S3Client, BackendConfig};
use angos_storage::fs::Backend as FsBackend;
use angos_storage::s3::Backend as S3Store;
use angos_storage::{Error as StorageError, ObjectStore, paginated};
use bytes::Bytes;
use futures_util::{TryStreamExt, stream};
use sha2::{Digest, Sha256};

use crate::error::{GateError, GateResult};

const LIST_PAGE_SIZE: u16 = 1000;
const SNAPSHOT_CONCURRENCY: usize = 32;
/// One full part at the s3 backend's default part size, so a seeded upload
/// opens a real multipart with no staged remainder left behind.
const MULTIPART_SEED_BYTES: usize = 5 * 1024 * 1024;

/// A ground-truth store snapshot: one `key -> sha256(content)` entry per
/// object. Gates diff snapshots byte-level instead of trusting scrub's own
/// accounting.
pub type Snapshot = BTreeMap<String, String>;

/// Direct byte-level access to the object store under test, fs or s3,
/// bypassing the registry API entirely.
pub struct GateStore {
    store: Arc<dyn ObjectStore>,
    s3: bool,
}

impl GateStore {
    /// Construct from the environment contract: `GATE_MODE=fs` with
    /// `GATE_DATA_ROOT`, or `GATE_MODE=s3` with `GATE_ENDPOINT` and
    /// `GATE_BUCKET` (credentials default to the CI rustfs ones).
    pub fn from_env() -> GateResult<Self> {
        let mode = require_env("GATE_MODE")?;
        let store: Arc<dyn ObjectStore> = match mode.as_str() {
            "fs" => {
                let root = require_env("GATE_DATA_ROOT")?;
                Arc::new(FsBackend::builder(root).build())
            }
            "s3" => {
                let config = BackendConfig {
                    endpoint: require_env("GATE_ENDPOINT")?,
                    bucket: require_env("GATE_BUCKET")?,
                    access_key_id: env_or("AWS_ACCESS_KEY_ID", "root"),
                    secret_key: env_or("AWS_SECRET_ACCESS_KEY", "roottoor"),
                    region: env_or("AWS_DEFAULT_REGION", "us-east-1"),
                    ..BackendConfig::default()
                };
                let client = S3Client::new(&config)?;
                Arc::new(S3Store::builder(Arc::new(client)).build())
            }
            other => {
                return Err(GateError::Environment(format!(
                    "GATE_MODE '{other}' is not fs or s3"
                )));
            }
        };
        Ok(Self {
            store,
            s3: mode == "s3",
        })
    }

    /// Whether the store under test speaks a multipart protocol (s3); the
    /// multipart gates are skipped on fs, which has none.
    pub fn is_s3(&self) -> bool {
        self.s3
    }

    /// Whether an object exists at `key` (a directory placeholder on fs also
    /// counts: a hollow container surviving a repair is itself a defect).
    pub async fn exists(&self, key: &str) -> GateResult<bool> {
        match self.store.head(key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// The object's bytes, or `None` if it does not exist.
    pub async fn body(&self, key: &str) -> GateResult<Option<Vec<u8>>> {
        match self.store.get(key).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Write raw bytes at `key`, creating or replacing the object.
    pub async fn put(&self, key: &str, body: impl Into<Bytes>) -> GateResult<()> {
        self.store.put(key, body.into()).await?;
        Ok(())
    }

    /// Delete the object at `key`; a missing key counts as success.
    pub async fn delete(&self, key: &str) -> GateResult<()> {
        self.store.delete(key).await?;
        Ok(())
    }

    /// Delete every object under the directory `prefix`.
    pub async fn delete_prefix(&self, prefix: &str) -> GateResult<()> {
        self.store.delete_prefix(prefix).await?;
        Ok(())
    }

    /// Every key under `prefix`, across pages.
    pub async fn keys_under(&self, prefix: &str) -> GateResult<Vec<String>> {
        paginated(|token| async move {
            let page = self.store.list(prefix, LIST_PAGE_SIZE, token).await?;
            Ok::<_, GateError>((page.items, page.next_token))
        })
        .try_collect()
        .await
    }

    /// Open a real in-flight multipart upload at `key` with one committed
    /// part and no session marker: exactly the wreckage a crash between
    /// opening the multipart and writing the marker leaves behind.
    pub async fn seed_orphan_multipart(&self, key: &str) -> GateResult<()> {
        let body = Bytes::from(vec![0u8; MULTIPART_SEED_BYTES]);
        let len = body.len() as u64;
        self.store.create_upload(key).await?;
        self.store
            .write_upload(key, Box::pin(stream::once(async { Ok(body) })), Some(len))
            .await?;
        Ok(())
    }

    /// Discard the upload at `key` and all backend state it owns.
    pub async fn abort_upload(&self, key: &str) -> GateResult<()> {
        self.store.abort_upload(key).await?;
        Ok(())
    }

    /// Count of in-flight multipart uploads store-wide (always 0 on fs).
    pub async fn multipart_count(&self) -> GateResult<usize> {
        let mut count = 0;
        let mut key_marker: Option<String> = None;
        let mut id_marker: Option<String> = None;
        loop {
            let page = self
                .store
                .list_multipart_uploads(key_marker.as_deref(), id_marker.as_deref())
                .await?;
            count += page.uploads.len();
            key_marker = page.next_key_marker;
            id_marker = page.next_upload_id_marker;
            if key_marker.is_none() && id_marker.is_none() {
                break;
            }
        }
        Ok(count)
    }

    /// Hash every object in the store. Content is re-read and re-hashed on
    /// every call so the snapshot trusts nothing cached; keys stream off the
    /// listing straight into up to `SNAPSHOT_CONCURRENCY` concurrent reads.
    pub async fn snapshot(&self) -> GateResult<Snapshot> {
        paginated(|token| async move {
            let page = self.store.list("", LIST_PAGE_SIZE, token).await?;
            Ok::<_, GateError>((page.items, page.next_token))
        })
        .map_ok(|key| {
            let store = Arc::clone(&self.store);
            async move {
                let body = store.get(&key).await?;
                Ok((key, sha256_hex(&body)))
            }
        })
        .try_buffer_unordered(SNAPSHOT_CONCURRENCY)
        .try_fold(Snapshot::new(), |mut snap, (key, hash)| async move {
            snap.insert(key, hash);
            Ok(snap)
        })
        .await
    }
}

/// Keys whose presence or content differs between two snapshots, sorted.
pub fn snapshot_diff(a: &Snapshot, b: &Snapshot) -> Vec<String> {
    let mut diff: Vec<String> = a
        .iter()
        .filter(|(key, hash)| b.get(*key) != Some(hash))
        .map(|(key, _)| key.clone())
        .collect();
    diff.extend(b.keys().filter(|key| !a.contains_key(*key)).cloned());
    diff.sort();
    diff.dedup();
    diff
}

/// Persist a snapshot as sorted `key<TAB>hash` lines for failure forensics.
pub fn write_snapshot(path: &Path, snap: &Snapshot) -> GateResult<()> {
    let mut out = String::new();
    for (key, hash) in snap {
        out.push_str(key);
        out.push('\t');
        out.push_str(hash);
        out.push('\n');
    }
    write(path, out)?;
    Ok(())
}

/// Hex-encoded sha256 of `data`, the digest form the OCI API uses.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn require_env(name: &str) -> GateResult<String> {
    env::var(name).map_err(|_| GateError::Environment(format!("{name} must be set")))
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}
