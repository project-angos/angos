//! S3 lock storage.
//!
//! Writes lock objects at `.tx-locks/<shard>/<key>` on a
//! [`ConditionalStore`](angos_storage::ConditionalStore) backend, using
//! `put_if_absent` for atomic acquire, `put_if_match` for heartbeat refresh,
//! and `delete_if_match` for release. The CAS capability gate guarantees the
//! provider enforces all three conditions and surfaces `ETag`s.
//!
//! ## Transport-error resilience
//!
//! A conditional write can fail with a *status-less* transport error (a dropped
//! connection or timeout) whose outcome is ambiguous: the request may already
//! have landed on the server. The s3-client deliberately does **not** blindly
//! retry such writes, because a replayed conditional `PUT` could observe a false
//! `PreconditionFailed` for a write that actually succeeded. Lock objects,
//! however, carry a unique per-write nonce, so after an ambiguous error this
//! layer reads the object back and disambiguates: a byte-for-byte match means
//! *our* write landed, a different body means a competitor owns it, and an
//! absent or unreadable object means we can safely retry. This restores
//! resilience to transient blips for the lock domain without weakening the
//! s3-client's guarantee for blob-data writes.

use std::{fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use sha2::{Digest as ShaDigest, Sha256};
use tokio::time::sleep;
use tracing::debug;

use angos_backoff::Backoff;
use angos_storage::{ConditionalStore, Error as StorageError, Etag};

use crate::lock::{
    Error,
    storage::{DeleteIfMatchOutcome, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome},
};

fn shard_prefix(key: &str) -> String {
    let hash = Sha256::digest(key.as_bytes());
    format!("{:02x}", hash[0])
}

/// Produce the S3 object path for a lock on `key`.
#[must_use]
pub fn lock_path(key: &str) -> String {
    let shard = shard_prefix(key);
    format!(".tx-locks/{shard}/{key}")
}

/// Maximum number of attempts (the initial write plus reconciling retries)
/// for a conditional lock write whose transport outcome is ambiguous.
const MAX_CONDITIONAL_ATTEMPTS: u32 = 3;

/// The `(body, ETag, last_modified)` read-back of a lock object, as returned by
/// [`ConditionalStore::get_with_metadata`].
type LockReadback = Result<(Vec<u8>, Option<Etag>, Option<DateTime<Utc>>), StorageError>;

/// Convert the `ETag` of a successful conditional write or read-back into the
/// [`LockStorage`] contract's mandatory fencing token. A missing `ETag` on a
/// CAS-verified backend is a hard anomaly, not a degraded mode.
fn require_etag(etag: Option<String>) -> Result<String, Error> {
    etag.ok_or_else(|| {
        Error::StorageBackend("conditional operation surfaced no ETag for lock object".to_string())
    })
}

/// Decision returned by [`reconcile_absent`] after reading a lock object back
/// following an ambiguous `put_if_absent` transport error.
enum AbsentReconcile {
    /// Our own create is on the server (byte-identical body): treat as created.
    Created(Option<String>),
    /// A different body is present: another holder owns the lock.
    AlreadyExists,
    /// The object is absent or could not be read: retry the conditional create.
    Retry,
}

/// Disambiguate an ambiguous `put_if_absent` by inspecting the read-back of the
/// lock object. `our_body` is the exact payload we attempted to write; because
/// it carries a unique nonce, a byte match proves *our* create landed (rather
/// than a competitor's identical-looking body).
fn reconcile_absent(readback: LockReadback, our_body: &[u8]) -> AbsentReconcile {
    match readback {
        Ok((stored, etag, _)) if stored.as_slice() == our_body => {
            AbsentReconcile::Created(etag.map(Etag::into_inner))
        }
        Ok(_) => AbsentReconcile::AlreadyExists,
        Err(_) => AbsentReconcile::Retry,
    }
}

/// Decision returned by [`reconcile_match`] after reading a lock object back
/// following an ambiguous `put_if_match` transport error.
enum MatchReconcile {
    /// Our own update is on the server (byte-identical body): treat as updated.
    Updated(Option<String>),
    /// Another writer changed the object: the conditional update lost.
    Mismatch,
    /// The precondition still holds (or the read failed): retry the update.
    Retry,
}

/// Disambiguate an ambiguous `put_if_match` by inspecting the read-back. A byte
/// match means our update landed; an unchanged `ETag` (still equal to
/// `expected_etag`) means it did not land but the precondition still holds, so a
/// retry is safe; anything else means another writer won the object.
fn reconcile_match(readback: LockReadback, our_body: &[u8], expected_etag: &str) -> MatchReconcile {
    match readback {
        Ok((stored, etag, _)) if stored.as_slice() == our_body => {
            MatchReconcile::Updated(etag.map(Etag::into_inner))
        }
        Ok((_, Some(etag), _)) if etag.as_str() == expected_etag => MatchReconcile::Retry,
        // A foreign/rotated object, or one that vanished, means another writer won.
        Ok(_) | Err(StorageError::NotFound) => MatchReconcile::Mismatch,
        // The read-back itself failed: we could not confirm, so retry.
        Err(_) => MatchReconcile::Retry,
    }
}

/// S3-backed lock storage.
///
/// Uses `put_if_absent` for atomic acquire, `put_if_match` for heartbeat
/// refresh, and `delete_if_match` for release, so a holder can only ever
/// remove its own lock object.
///
/// Constructed via [`S3LockStorage::new`].
#[derive(Clone)]
pub struct S3LockStorage {
    store: Arc<dyn ConditionalStore>,
    conditional_retry: Backoff,
}

impl Debug for S3LockStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3LockStorage").finish_non_exhaustive()
    }
}

impl S3LockStorage {
    /// Construct an `S3LockStorage` from an existing conditional store.
    #[must_use]
    pub fn new(store: Arc<dyn ConditionalStore>) -> Self {
        Self {
            store,
            conditional_retry: Backoff::constant(Duration::from_millis(25)).with_jitter(),
        }
    }
}

#[async_trait]
impl LockStorage for S3LockStorage {
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutIfAbsentOutcome, Error> {
        let path = lock_path(key);
        let body = Bytes::from(body);
        let mut attempts: u32 = 0;
        loop {
            attempts += 1;
            match self.store.put_if_absent(&path, body.clone()).await {
                Ok(etag) => {
                    return Ok(PutIfAbsentOutcome::Created(require_etag(
                        etag.map(Etag::into_inner),
                    )?));
                }
                Err(StorageError::PreconditionFailed) => {
                    return Ok(PutIfAbsentOutcome::AlreadyExists);
                }
                Err(e) => {
                    if attempts >= MAX_CONDITIONAL_ATTEMPTS {
                        return Err(Error::StorageBackend(e.to_string()));
                    }
                    // Ambiguous backend error (typically a status-less transport
                    // blip): the create may or may not have landed. Read the
                    // object back and disambiguate via the unique body nonce.
                    match reconcile_absent(self.store.get_with_metadata(&path).await, body.as_ref())
                    {
                        AbsentReconcile::Created(etag) => {
                            return Ok(PutIfAbsentOutcome::Created(require_etag(etag)?));
                        }
                        AbsentReconcile::AlreadyExists => {
                            return Ok(PutIfAbsentOutcome::AlreadyExists);
                        }
                        AbsentReconcile::Retry => {
                            debug!(key, attempts, error = %e, "S3LockStorage: ambiguous acquire write, retrying");
                            sleep(self.conditional_retry.delay(0)).await;
                        }
                    }
                }
            }
        }
    }

    async fn put_if_match(
        &self,
        key: &str,
        expected_etag: &str,
        body: Vec<u8>,
    ) -> Result<PutIfMatchOutcome, Error> {
        let path = lock_path(key);
        let etag = Etag::new(expected_etag);
        let body = Bytes::from(body);
        let mut attempts: u32 = 0;
        loop {
            attempts += 1;
            match self.store.put_if_match(&path, &etag, body.clone()).await {
                Ok(new_etag) => {
                    return Ok(PutIfMatchOutcome::Updated(require_etag(
                        new_etag.map(Etag::into_inner),
                    )?));
                }
                Err(StorageError::PreconditionFailed) => return Ok(PutIfMatchOutcome::Mismatch),
                Err(e) => {
                    if attempts >= MAX_CONDITIONAL_ATTEMPTS {
                        return Err(Error::StorageBackend(e.to_string()));
                    }
                    match reconcile_match(
                        self.store.get_with_metadata(&path).await,
                        body.as_ref(),
                        expected_etag,
                    ) {
                        MatchReconcile::Updated(new_etag) => {
                            return Ok(PutIfMatchOutcome::Updated(require_etag(new_etag)?));
                        }
                        MatchReconcile::Mismatch => return Ok(PutIfMatchOutcome::Mismatch),
                        MatchReconcile::Retry => {
                            debug!(key, attempts, error = %e, "S3LockStorage: ambiguous refresh write, retrying");
                            sleep(self.conditional_retry.delay(0)).await;
                        }
                    }
                }
            }
        }
    }

    async fn get_with_etag(
        &self,
        key: &str,
    ) -> Result<(Vec<u8>, String, Option<DateTime<Utc>>), Error> {
        let path = lock_path(key);
        let mut attempts: u32 = 0;
        loop {
            attempts += 1;
            match self.store.get_with_metadata(&path).await {
                Ok((body, etag, last_modified)) => {
                    return Ok((
                        body,
                        require_etag(etag.map(Etag::into_inner))?,
                        last_modified,
                    ));
                }
                Err(StorageError::NotFound) => return Err(Error::NotFound),
                Err(e) => {
                    // A read is idempotent, so a transport blip can simply be
                    // retried. This backstops the s3-client's own retries under
                    // sustained pressure so that a stale-lock-recovery read (or a
                    // heartbeat re-read) does not fail an acquire (and therefore
                    // a blob upload or delete) on a transient error.
                    if attempts >= MAX_CONDITIONAL_ATTEMPTS {
                        return Err(Error::StorageBackend(e.to_string()));
                    }
                    debug!(key, attempts, error = %e, "S3LockStorage: ambiguous lock read, retrying");
                    sleep(self.conditional_retry.delay(0)).await;
                }
            }
        }
    }

    async fn delete_if_match(
        &self,
        key: &str,
        expected_etag: &str,
    ) -> Result<DeleteIfMatchOutcome, Error> {
        let path = lock_path(key);
        let etag = Etag::new(expected_etag);
        let mut attempts: u32 = 0;
        loop {
            attempts += 1;
            match self.store.delete_if_match(&path, &etag).await {
                Err(StorageError::PreconditionFailed) => return Ok(DeleteIfMatchOutcome::Mismatch),
                Ok(()) | Err(StorageError::NotFound) => return Ok(DeleteIfMatchOutcome::Deleted),
                Err(e) => {
                    // A conditional delete is safe to replay after an ambiguous
                    // transport error: if the first attempt landed, the retry
                    // observes an absent object (or a foreign successor) and
                    // never deletes anything that is not ours.
                    if attempts >= MAX_CONDITIONAL_ATTEMPTS {
                        return Err(Error::StorageBackend(e.to_string()));
                    }
                    debug!(key, attempts, error = %e, "S3LockStorage: ambiguous conditional delete, retrying");
                    sleep(self.conditional_retry.delay(0)).await;
                }
            }
        }
    }

    fn label(&self) -> &'static str {
        "s3"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use angos_storage::{
        ConditionalStore, Error as StorageError, Etag, MemoryObjectStore,
        test_util::{HookedStore, StoreHook, StoreOp},
    };
    use async_trait::async_trait;
    use bytes::Bytes;

    use crate::lock::{
        Error,
        storage::{
            DeleteIfMatchOutcome, LockStorage, PutIfAbsentOutcome, PutIfMatchOutcome,
            s3::{
                AbsentReconcile, MAX_CONDITIONAL_ATTEMPTS, MatchReconcile, S3LockStorage,
                lock_path, reconcile_absent, reconcile_match,
            },
        },
    };

    /// `MemoryObjectStore` implements `ConditionalStore`, so it stands in for a
    /// CAS-capable S3 backend with no live infrastructure.
    fn storage() -> S3LockStorage {
        S3LockStorage::new(Arc::new(MemoryObjectStore::new()))
    }

    #[tokio::test]
    async fn put_if_absent_created_then_already_exists() {
        let s = storage();

        let first = s.put_if_absent("k", b"body-1".to_vec()).await.unwrap();
        assert!(
            matches!(first, PutIfAbsentOutcome::Created(_)),
            "first put_if_absent must create and return an ETag"
        );

        let second = s.put_if_absent("k", b"body-2".to_vec()).await.unwrap();
        assert!(
            matches!(second, PutIfAbsentOutcome::AlreadyExists),
            "a second put_if_absent on the same key must report AlreadyExists"
        );
    }

    #[tokio::test]
    async fn put_if_match_updates_on_matching_etag_and_mismatches_on_stale() {
        let s = storage();

        let PutIfAbsentOutcome::Created(etag) = s.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created");
        };

        let updated = s.put_if_match("k", &etag, b"v2".to_vec()).await.unwrap();
        let new_etag = match updated {
            PutIfMatchOutcome::Updated(e) => e,
            PutIfMatchOutcome::Mismatch => panic!("expected Updated(etag), got Mismatch"),
        };
        assert_ne!(
            new_etag, etag,
            "a successful put_if_match must rotate the ETag"
        );

        // The original (now stale) ETag must mismatch.
        let stale = s.put_if_match("k", &etag, b"v3".to_vec()).await.unwrap();
        assert!(
            matches!(stale, PutIfMatchOutcome::Mismatch),
            "put_if_match against a stale ETag must report Mismatch"
        );
    }

    #[tokio::test]
    async fn get_with_etag_returns_body_and_etag_and_not_found_when_absent() {
        let s = storage();

        let absent = s.get_with_etag("missing").await;
        assert!(
            matches!(absent, Err(Error::NotFound)),
            "get_with_etag on an absent key must be NotFound"
        );

        let PutIfAbsentOutcome::Created(etag) =
            s.put_if_absent("k", b"the-body".to_vec()).await.unwrap()
        else {
            panic!("expected Created");
        };

        let (body, read_etag, _last_modified) = s.get_with_etag("k").await.unwrap();
        assert_eq!(body, b"the-body");
        assert_eq!(
            read_etag, etag,
            "get_with_etag must surface the same ETag the put returned"
        );
    }

    #[tokio::test]
    async fn delete_if_match_deletes_on_match_and_mismatches_on_stale() {
        let s = storage();

        let PutIfAbsentOutcome::Created(etag) = s.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created");
        };

        // Rotate the live ETag so the original is stale.
        let PutIfMatchOutcome::Updated(_new_etag) =
            s.put_if_match("k", &etag, b"v2".to_vec()).await.unwrap()
        else {
            panic!("expected Updated");
        };

        // Deleting with the now-stale ETag must mismatch and leave the object.
        let stale = s.delete_if_match("k", &etag).await.unwrap();
        assert!(
            matches!(stale, DeleteIfMatchOutcome::Mismatch),
            "delete_if_match against a stale ETag must report Mismatch"
        );
        assert!(
            s.get_with_etag("k").await.is_ok(),
            "a mismatched conditional delete must leave the object in place"
        );

        // Delete with the current ETag succeeds, then the key is gone.
        let (_body, current_etag, _) = s.get_with_etag("k").await.unwrap();
        let deleted = s.delete_if_match("k", &current_etag).await.unwrap();
        assert!(
            matches!(deleted, DeleteIfMatchOutcome::Deleted),
            "delete_if_match against the current ETag must delete"
        );
        assert!(
            matches!(s.get_with_etag("k").await, Err(Error::NotFound)),
            "the key must be gone after a successful conditional delete"
        );
    }

    // reconcile decision helpers (pure)

    fn mk_etag(s: &str) -> Etag {
        Etag::new(s)
    }

    #[test]
    fn reconcile_absent_byte_match_is_created() {
        let our = b"mine".to_vec();
        let out = reconcile_absent(Ok((our.clone(), Some(mk_etag("\"e1\"")), None)), &our);
        assert!(
            matches!(out, AbsentReconcile::Created(Some(e)) if e == "\"e1\""),
            "a byte-identical read-back proves our own create landed"
        );
    }

    #[test]
    fn reconcile_absent_different_body_is_already_exists() {
        let out = reconcile_absent(
            Ok((b"theirs".to_vec(), Some(mk_etag("\"e1\"")), None)),
            b"mine",
        );
        assert!(
            matches!(out, AbsentReconcile::AlreadyExists),
            "a different body means a competitor holds the lock"
        );
    }

    #[test]
    fn reconcile_absent_not_found_is_retry() {
        let out = reconcile_absent(Err(StorageError::NotFound), b"mine");
        assert!(
            matches!(out, AbsentReconcile::Retry),
            "an absent object means the create did not land: retry"
        );
    }

    #[test]
    fn reconcile_absent_backend_error_is_retry() {
        let out = reconcile_absent(Err(StorageError::Backend("blip".into())), b"mine");
        assert!(
            matches!(out, AbsentReconcile::Retry),
            "an unreadable object cannot confirm the outcome: retry"
        );
    }

    #[test]
    fn reconcile_match_byte_match_is_updated() {
        let our = b"mine".to_vec();
        let out = reconcile_match(
            Ok((our.clone(), Some(mk_etag("\"new\"")), None)),
            &our,
            "\"old\"",
        );
        assert!(
            matches!(out, MatchReconcile::Updated(Some(e)) if e == "\"new\""),
            "a byte-identical read-back proves our update landed"
        );
    }

    #[test]
    fn reconcile_match_unchanged_etag_is_retry() {
        // The stored body is not ours and the ETag still equals the precondition:
        // our write did not land, but the precondition holds, so a retry is safe.
        let out = reconcile_match(
            Ok((b"old-body".to_vec(), Some(mk_etag("\"old\"")), None)),
            b"mine",
            "\"old\"",
        );
        assert!(matches!(out, MatchReconcile::Retry));
    }

    #[test]
    fn reconcile_match_rotated_etag_is_mismatch() {
        let out = reconcile_match(
            Ok((b"theirs".to_vec(), Some(mk_etag("\"rotated\"")), None)),
            b"mine",
            "\"old\"",
        );
        assert!(
            matches!(out, MatchReconcile::Mismatch),
            "a foreign body under a rotated ETag means another writer won"
        );
    }

    #[test]
    fn reconcile_match_not_found_is_mismatch() {
        let out = reconcile_match(Err(StorageError::NotFound), b"mine", "\"old\"");
        assert!(
            matches!(out, MatchReconcile::Mismatch),
            "a vanished object can never satisfy an If-Match update"
        );
    }

    #[test]
    fn reconcile_match_backend_error_is_retry() {
        let out = reconcile_match(
            Err(StorageError::Backend("blip".into())),
            b"mine",
            "\"old\"",
        );
        assert!(matches!(out, MatchReconcile::Retry));
    }

    // fault-injecting hook for end-to-end retry tests

    /// One injected fault for a conditional write.
    #[derive(Clone, Copy)]
    enum Fault {
        /// Perform the inner write (so it lands) but return a transport error,
        /// modelling a lost acknowledgement.
        LandThenError,
        /// Return a transport error without writing anything.
        ErrorNoLand,
    }

    /// Injects transport faults into the conditional calls of a wrapped
    /// [`MemoryObjectStore`]. `inner` is a clone sharing the wrapped store's
    /// map, so a `LandThenError` fault can land the write itself before
    /// failing, and tests can seed or inspect objects directly.
    struct FaultScript {
        inner: MemoryObjectStore,
        absent_faults: Mutex<VecDeque<Fault>>,
        match_faults: Mutex<VecDeque<Fault>>,
        get_faults: Mutex<VecDeque<Fault>>,
        delete_faults: Mutex<VecDeque<Fault>>,
        absent_calls: AtomicUsize,
        match_calls: AtomicUsize,
        get_calls: AtomicUsize,
        delete_calls: AtomicUsize,
    }

    impl FaultScript {
        fn set_get_faults(&self, faults: &[Fault]) {
            *self.get_faults.lock().unwrap() = faults.iter().copied().collect();
        }

        fn set_delete_faults(&self, faults: &[Fault]) {
            *self.delete_faults.lock().unwrap() = faults.iter().copied().collect();
        }
    }

    fn injected() -> StorageError {
        StorageError::Backend("injected transport error".into())
    }

    type FlakyStore = HookedStore<Arc<dyn ConditionalStore>, FaultScript>;

    fn flaky_store(absent: &[Fault], refresh: &[Fault]) -> Arc<FlakyStore> {
        let inner = MemoryObjectStore::new();
        let script = FaultScript {
            inner: inner.clone(),
            absent_faults: Mutex::new(absent.iter().copied().collect()),
            match_faults: Mutex::new(refresh.iter().copied().collect()),
            get_faults: Mutex::new(VecDeque::new()),
            delete_faults: Mutex::new(VecDeque::new()),
            absent_calls: AtomicUsize::new(0),
            match_calls: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
        };
        Arc::new(HookedStore::new(Arc::new(inner), script))
    }

    #[async_trait]
    impl StoreHook for FaultScript {
        async fn before(&self, op: StoreOp<'_>) -> Result<(), StorageError> {
            match op {
                StoreOp::GetWithEtag { .. } => {
                    self.get_calls.fetch_add(1, Ordering::SeqCst);
                    let fault = self.get_faults.lock().unwrap().pop_front();
                    if fault.is_some() {
                        return Err(injected());
                    }
                    Ok(())
                }
                StoreOp::PutIfAbsent { key, data } => {
                    self.absent_calls.fetch_add(1, Ordering::SeqCst);
                    let fault = self.absent_faults.lock().unwrap().pop_front();
                    match fault {
                        Some(Fault::LandThenError) => {
                            let _ = self.inner.put_if_absent(key, data.clone()).await;
                            Err(injected())
                        }
                        Some(Fault::ErrorNoLand) => Err(injected()),
                        None => Ok(()),
                    }
                }
                StoreOp::PutIfMatch { key, etag, data } => {
                    self.match_calls.fetch_add(1, Ordering::SeqCst);
                    let fault = self.match_faults.lock().unwrap().pop_front();
                    match fault {
                        Some(Fault::LandThenError) => {
                            let _ = self.inner.put_if_match(key, etag, data.clone()).await;
                            Err(injected())
                        }
                        Some(Fault::ErrorNoLand) => Err(injected()),
                        None => Ok(()),
                    }
                }
                StoreOp::DeleteIfMatch { key, etag } => {
                    self.delete_calls.fetch_add(1, Ordering::SeqCst);
                    let fault = self.delete_faults.lock().unwrap().pop_front();
                    match fault {
                        Some(Fault::LandThenError) => {
                            let _ = self.inner.delete_if_match(key, etag).await;
                            Err(injected())
                        }
                        Some(Fault::ErrorNoLand) => Err(injected()),
                        None => Ok(()),
                    }
                }
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn put_if_absent_lost_ack_landed_returns_created() {
        // The PUT lands but the ack is lost (transport error). The reconcile
        // read-back finds our own body, so the acquire succeeds: the exact
        // regression this layer repairs.
        let store = flaky_store(&[Fault::LandThenError], &[]);
        let lock = S3LockStorage::new(store.clone());

        let outcome = lock.put_if_absent("k", b"the-body".to_vec()).await.unwrap();

        assert!(
            matches!(outcome, PutIfAbsentOutcome::Created(_)),
            "a transport error on a PUT that landed must still report Created, not an error"
        );
        assert_eq!(store.hook().absent_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.hook().get_calls.load(Ordering::SeqCst),
            1,
            "exactly one reconcile read-back confirms ownership"
        );
    }

    #[tokio::test]
    async fn put_if_absent_transport_error_without_landing_retries_then_creates() {
        let store = flaky_store(&[Fault::ErrorNoLand], &[]);
        let lock = S3LockStorage::new(store.clone());

        let outcome = lock.put_if_absent("k", b"the-body".to_vec()).await.unwrap();

        assert!(matches!(outcome, PutIfAbsentOutcome::Created(_)));
        assert_eq!(
            store.hook().absent_calls.load(Ordering::SeqCst),
            2,
            "a non-landing transport error retries the conditional create once"
        );
    }

    #[tokio::test]
    async fn put_if_absent_competitor_after_transport_error_is_already_exists() {
        // Seed a competitor's lock object, then fault our acquire's PUT.
        let store = flaky_store(&[Fault::ErrorNoLand], &[]);
        store
            .hook()
            .inner
            .put_if_absent(&lock_path("k"), Bytes::from_static(b"competitor"))
            .await
            .unwrap();
        let lock = S3LockStorage::new(store.clone());

        let outcome = lock.put_if_absent("k", b"mine".to_vec()).await.unwrap();

        assert!(
            matches!(outcome, PutIfAbsentOutcome::AlreadyExists),
            "a foreign body on the read-back means another holder owns the lock"
        );
    }

    #[tokio::test]
    async fn put_if_absent_genuine_precondition_is_terminal() {
        // No faults: the second acquire hits a real PreconditionFailed and must
        // NOT reconcile or retry.
        let store = flaky_store(&[], &[]);
        let lock = S3LockStorage::new(store.clone());

        lock.put_if_absent("k", b"first".to_vec()).await.unwrap();
        let gets_before = store.hook().get_calls.load(Ordering::SeqCst);
        let outcome = lock.put_if_absent("k", b"second".to_vec()).await.unwrap();

        assert!(matches!(outcome, PutIfAbsentOutcome::AlreadyExists));
        assert_eq!(
            store.hook().get_calls.load(Ordering::SeqCst),
            gets_before,
            "a genuine PreconditionFailed must be terminal: no reconcile read-back"
        );
    }

    #[tokio::test]
    async fn put_if_absent_budget_exhausted_propagates_error() {
        let faults = vec![Fault::ErrorNoLand; MAX_CONDITIONAL_ATTEMPTS as usize];
        let store = flaky_store(&faults, &[]);
        let lock = S3LockStorage::new(store.clone());

        let result = lock.put_if_absent("k", b"the-body".to_vec()).await;

        assert!(
            matches!(result, Err(Error::StorageBackend(_))),
            "a persistent transport error must surface after the bounded retries"
        );
        assert_eq!(
            store.hook().absent_calls.load(Ordering::SeqCst),
            MAX_CONDITIONAL_ATTEMPTS as usize,
            "the conditional create is attempted exactly MAX_CONDITIONAL_ATTEMPTS times"
        );
    }

    #[tokio::test]
    async fn put_if_match_lost_ack_landed_returns_updated() {
        // Create the object, capture its ETag, then fault a refresh that lands.
        let store = flaky_store(&[], &[Fault::LandThenError]);
        let lock = S3LockStorage::new(store.clone());

        let PutIfAbsentOutcome::Created(etag) =
            lock.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created");
        };

        let outcome = lock.put_if_match("k", &etag, b"v2".to_vec()).await.unwrap();

        assert!(
            matches!(outcome, PutIfMatchOutcome::Updated(_)),
            "a transport error on a refresh that landed must still report Updated"
        );
    }

    #[tokio::test]
    async fn put_if_match_takeover_during_transport_error_is_mismatch() {
        let store = flaky_store(&[], &[Fault::ErrorNoLand]);
        let lock = S3LockStorage::new(store.clone());

        let PutIfAbsentOutcome::Created(stale_etag) =
            lock.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created");
        };

        // A competitor rotates the object's ETag and body out from under us.
        let (_body, live_etag) = store
            .hook()
            .inner
            .get_with_etag(&lock_path("k"))
            .await
            .unwrap();
        store
            .hook()
            .inner
            .put_if_match(
                &lock_path("k"),
                &live_etag.expect("etag present"),
                Bytes::from_static(b"takeover"),
            )
            .await
            .unwrap();

        // Our refresh against the now-stale ETag faults; the read-back shows a
        // rotated ETag and a foreign body, so ownership is lost.
        let outcome = lock
            .put_if_match("k", &stale_etag, b"v2".to_vec())
            .await
            .unwrap();

        assert!(
            matches!(outcome, PutIfMatchOutcome::Mismatch),
            "a transport error whose read-back shows a rotated ETag is a Mismatch"
        );
    }

    #[tokio::test]
    async fn delete_if_match_transport_error_without_landing_retries_then_deletes() {
        let store = flaky_store(&[], &[]);
        let lock = S3LockStorage::new(store.clone());
        let PutIfAbsentOutcome::Created(etag) =
            lock.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created");
        };

        store.hook().set_delete_faults(&[Fault::ErrorNoLand]);
        let outcome = lock.delete_if_match("k", &etag).await.unwrap();

        assert!(
            matches!(outcome, DeleteIfMatchOutcome::Deleted),
            "a non-landing transport error retries the conditional delete once"
        );
        assert_eq!(store.hook().delete_calls.load(Ordering::SeqCst), 2);
        assert!(
            matches!(lock.get_with_etag("k").await, Err(Error::NotFound)),
            "the lock object must be gone after the retried delete"
        );
    }

    #[tokio::test]
    async fn delete_if_match_lost_ack_landed_reports_deleted() {
        // The DELETE lands but the ack is lost. The replay observes an absent
        // object, which counts as Deleted; nothing foreign is ever removed.
        let store = flaky_store(&[], &[]);
        let lock = S3LockStorage::new(store.clone());
        let PutIfAbsentOutcome::Created(etag) =
            lock.put_if_absent("k", b"v1".to_vec()).await.unwrap()
        else {
            panic!("expected Created");
        };

        store.hook().set_delete_faults(&[Fault::LandThenError]);
        let outcome = lock.delete_if_match("k", &etag).await.unwrap();

        assert!(
            matches!(outcome, DeleteIfMatchOutcome::Deleted),
            "a transport error on a DELETE that landed must still report Deleted"
        );
        assert_eq!(store.hook().delete_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn get_with_etag_retries_transient_read_error_then_succeeds() {
        // A stale-lock-recovery read (the path behind the DELETE 500) hits a
        // transport blip, then succeeds, so the acquire must not fail.
        let store = flaky_store(&[], &[]);
        store
            .hook()
            .inner
            .put_if_absent(&lock_path("k"), Bytes::from_static(b"held"))
            .await
            .unwrap();
        store.hook().set_get_faults(&[Fault::ErrorNoLand]);
        let lock = S3LockStorage::new(store.clone());

        let (body, _etag, _) = lock.get_with_etag("k").await.unwrap();

        assert_eq!(
            body, b"held",
            "the read must recover after a transient blip"
        );
        assert_eq!(
            store.hook().get_calls.load(Ordering::SeqCst),
            2,
            "one transient read error is retried exactly once before succeeding"
        );
    }

    #[tokio::test]
    async fn get_with_etag_read_budget_exhausted_propagates_error() {
        let store = flaky_store(&[], &[]);
        store
            .hook()
            .set_get_faults(&vec![Fault::ErrorNoLand; MAX_CONDITIONAL_ATTEMPTS as usize]);
        let lock = S3LockStorage::new(store.clone());

        let result = lock.get_with_etag("k").await;

        assert!(
            matches!(result, Err(Error::StorageBackend(_))),
            "a persistent read transport error surfaces after the bounded retries"
        );
        assert_eq!(
            store.hook().get_calls.load(Ordering::SeqCst),
            MAX_CONDITIONAL_ATTEMPTS as usize,
            "the read is attempted exactly MAX_CONDITIONAL_ATTEMPTS times"
        );
    }
}
