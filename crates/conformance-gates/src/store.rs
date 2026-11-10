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
use futures_util::TryStreamExt;
use futures_util::stream;
use sha2::{Digest, Sha256};

use crate::error::{GateError, GateResult};

const LIST_PAGE_SIZE: u16 = 1000;
const SNAPSHOT_CONCURRENCY: usize = 32;
/// One full part at the s3 backend's default part size, so a seeded upload
/// opens a real multipart with no staged remainder left behind.
const MULTIPART_SEED_BYTES: usize = 5 * 1024 * 1024;

/// `key -> sha256(content)` for every object.
pub type Snapshot = BTreeMap<String, String>;

/// Direct byte-level access to the object store under test, bypassing the
/// registry API.
pub struct GateStore {
    pub objects: Arc<dyn ObjectStore>,
    /// The multipart gates are skipped on fs, which has no multipart protocol.
    pub is_s3: bool,
}

impl GateStore {
    /// S3 credentials default to the CI rustfs ones.
    pub fn from_env() -> GateResult<Self> {
        let mode = require_env("GATE_MODE")?;
        let objects: Arc<dyn ObjectStore> = match mode.as_str() {
            "fs" => {
                let root = require_env("GATE_DATA_ROOT")?;
                Arc::new(FsBackend::new(Path::new(&root), false))
            }
            "s3" => {
                let config = BackendConfig {
                    endpoint: require_env("GATE_ENDPOINT")?,
                    bucket: require_env("GATE_BUCKET")?,
                    access_key_id: env::var("AWS_ACCESS_KEY_ID")
                        .unwrap_or_else(|_| "root".to_string()),
                    secret_key: env::var("AWS_SECRET_ACCESS_KEY")
                        .unwrap_or_else(|_| "roottoor".to_string()),
                    region: env::var("AWS_DEFAULT_REGION")
                        .unwrap_or_else(|_| "us-east-1".to_string()),
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
            objects,
            is_s3: mode == "s3",
        })
    }

    /// A directory placeholder on fs counts: a hollow container surviving a
    /// repair is itself a defect.
    pub async fn exists(&self, key: &str) -> GateResult<bool> {
        match self.objects.head(key).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn body(&self, key: &str) -> GateResult<Option<Vec<u8>>> {
        match self.objects.get(key).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn keys_under(&self, prefix: &str) -> GateResult<Vec<String>> {
        paginated(|token| async move {
            Ok::<_, GateError>(self.objects.list(prefix, LIST_PAGE_SIZE, token).await?)
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
        self.objects
            .write_upload(key, Box::pin(stream::once(async { Ok(body) })), Some(len))
            .await?;
        Ok(())
    }

    /// In-flight multipart uploads store-wide, always 0 on fs.
    pub async fn multipart_count(&self) -> GateResult<usize> {
        let mut count = 0;
        let mut key_marker: Option<String> = None;
        let mut id_marker: Option<String> = None;
        loop {
            let page = self
                .objects
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

    /// Re-reads and re-hashes every object, trusting nothing cached.
    pub async fn snapshot(&self) -> GateResult<Snapshot> {
        paginated(|token| async move {
            Ok::<_, GateError>(self.objects.list("", LIST_PAGE_SIZE, token).await?)
        })
        .map_ok(|key| {
            let store = Arc::clone(&self.objects);
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
    diff
}

/// Sorted `key<TAB>hash` lines, for failure forensics.
pub fn write_snapshot(path: &Path, snap: &Snapshot) -> GateResult<()> {
    let out = snap
        .iter()
        .map(|(key, hash)| format!("{key}\t{hash}\n"))
        .collect::<Vec<_>>()
        .concat();
    write(path, out)?;
    Ok(())
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn require_env(name: &str) -> GateResult<String> {
    env::var(name).map_err(|_| GateError::Environment(format!("{name} must be set")))
}
