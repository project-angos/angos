//! Transaction executor trait and outcome type.

pub mod cas;
pub mod common;
pub mod locked;

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, info};
use uuid::Uuid;

use angos_storage::{ConditionalStore, ObjectStore};

#[cfg(feature = "redis")]
use crate::lock::storage::redis::RedisLockStorage;
use crate::{
    error::Error,
    executor::{cas::CasExecutor, locked::LockedExecutor},
    lock::{
        LockSession, LockStrategy,
        primitive::Lock,
        storage::{LockStorage, memory::MemoryLockStorage, s3::S3LockStorage},
    },
    transaction::Transaction,
};

/// Default retry budget for [`execute_with_retry`].
///
/// Subsystems pass this value (or a custom one) to the retry helper instead
/// of maintaining their own retry constants.
pub const DEFAULT_RETRY_BUDGET: u32 = 10;

/// The result of a successfully committed transaction.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The unique identifier of the committed transaction.
    pub tx_id: Uuid,
}

/// Drives a [`Transaction`] through Build → Prepare → Commit-intent →
/// Apply → Reap.
///
/// Both executors (`LockedExecutor` and `CasExecutor`) implement this trait;
/// callers build the same `Transaction` value and submit it to whichever
/// executor the deployment is configured to use.
#[async_trait]
pub trait TransactionExecutor: Send + Sync {
    /// Execute `tx` and return the commit [`Outcome`] on success.
    ///
    /// The executor manages whatever locking it needs internally (the Locked
    /// executor acquires distributed locks on the transaction's lock set; the
    /// CAS executor relies on conditional storage operations and acquires no
    /// transaction-scoped lock). Callers that hold their own [`LockSession`]
    /// (for example, the durable job consumer's per-`lock_key` execution
    /// lock) keep that session alive across this call and release it
    /// explicitly afterwards.
    ///
    /// # Errors
    ///
    /// - [`Error::Conflict`]: the transaction's read set or preconditions
    ///   were not met; the caller should rebuild and retry.
    /// - [`Error::Precondition`]: a CAS precondition failed during Apply and
    ///   the transaction was rolled back.
    /// - [`Error::Lock`]: a lock could not be acquired within the retry
    ///   budget.
    /// - [`Error::Storage`]: an underlying storage operation failed.
    async fn execute(&self, tx: Transaction) -> Result<Outcome, Error>;

    /// Non-blocking single-attempt lock acquire over the engine's internal lock.
    ///
    /// Returns `Ok(Some(session))` when all `keys` were acquired without
    /// contention. Returns `Ok(None)` when any key is already held; the caller
    /// should skip this job and move on. Returns `Err` only on a hard storage
    /// error.
    ///
    /// The returned [`LockSession`] is owned by the caller; it must be
    /// released via [`LockSession::release`] when the caller is done.
    async fn try_acquire(&self, keys: &[String]) -> Result<Option<LockSession>, Error>;

    /// Blocking acquire over the engine's internal lock.
    ///
    /// Retries on contention up to the lock's configured `max_retries` limit.
    /// The returned [`LockSession`] is owned by the caller; it must be
    /// released via [`LockSession::release`] when the caller is done.
    async fn acquire(&self, keys: &[String]) -> Result<LockSession, Error>;

    /// Returns the lock primitive the executor uses internally; the recovery
    /// loop and lock janitor use it to take ownership of stale intents and
    /// reclaim cold lock objects.
    fn lock(&self) -> Arc<Lock>;

    /// Returns the conditional store the executor uses, or `None` for the
    /// Locked executor; the recovery loop uses it to replay with the same
    /// conditional primitives the healthy path used.
    fn conditional_store(&self) -> Option<Arc<dyn ConditionalStore>>;
}

/// Execute a transaction and a caller-defined payload built by `build`,
/// retrying on [`Error::Conflict`] or [`Error::Precondition`] up to
/// `max_attempts` additional times.
///
/// `build` is called once before each attempt so the transaction can
/// incorporate fresh state on every retry. A [`Error::Conflict`] or
/// [`Error::Precondition`] returned by `build` itself (a builder that
/// revalidates its reads reports staleness this way) consumes an attempt
/// like an execute conflict; any other `build` error is propagated
/// immediately.
///
/// The closure returns `(Transaction, T)` so callers can thread any per-attempt
/// value out of the retry loop without needing shared mutable state.
///
/// Returns [`Error::Conflict`] when all attempts are exhausted.
///
/// # Errors
///
/// Returns the first non-retriable error from `build` or `executor.execute`.
/// Returns [`Error::Conflict`] once `max_attempts` retriable conflicts are
/// exhausted.
pub async fn execute_with_retry_payload<E, F, Fut, T>(
    executor: &E,
    mut build: F,
    max_attempts: u32,
) -> Result<(Outcome, T), Error>
where
    E: TransactionExecutor + ?Sized,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(Transaction, T), Error>> + Send,
    T: Send,
{
    let mut attempts = 0u32;
    loop {
        match build().await {
            Ok((tx, payload)) => match executor.execute(tx).await {
                Ok(o) => return Ok((o, payload)),
                Err(Error::Conflict | Error::Precondition) if attempts < max_attempts => {
                    debug!(attempts, max_attempts, "Transaction conflict, retrying");
                    attempts += 1;
                }
                Err(e) => return Err(e),
            },
            Err(Error::Conflict | Error::Precondition) if attempts < max_attempts => {
                debug!(attempts, max_attempts, "Transaction build conflict, retrying");
                attempts += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Execute a transaction built by `build`, retrying on [`Error::Conflict`]
/// or [`Error::Precondition`] up to `max_attempts` additional times; the
/// payload-less form of [`execute_with_retry_payload`], with the same retry
/// semantics for conflicts reported by `build` itself.
///
/// Returns [`Error::Conflict`] when all attempts are exhausted.
///
/// # Errors
///
/// Returns the first non-retriable error from `build` or `executor.execute`.
/// Returns [`Error::Conflict`] once `max_attempts` retriable conflicts are
/// exhausted.
///
/// # Example
///
/// ```rust,ignore
/// execute_with_retry(
///     executor.as_ref(),
///     || async { Ok(build_my_tx().await?) },
///     DEFAULT_RETRY_BUDGET,
/// ).await?;
/// ```
pub async fn execute_with_retry<E, F, Fut>(
    executor: &E,
    mut build: F,
    max_attempts: u32,
) -> Result<Outcome, Error>
where
    E: TransactionExecutor + ?Sized,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Transaction, Error>> + Send,
{
    let build = move || {
        let fut = build();
        async move { Ok((fut.await?, ())) }
    };
    let (outcome, ()) = execute_with_retry_payload(executor, build, max_attempts).await?;
    Ok(outcome)
}

// Executor factory: hides lock-storage and executor strategy from callers

/// Build a [`TransactionExecutor`] from operator-level inputs.
///
/// This is the engine's single seam for executor construction. Subsystems
/// (`metadata_store`, `job_store`, `blob_store`) call this with their
/// `ObjectStore` (and optional `ConditionalStore`) plus the operator's
/// [`LockStrategy`]; they never instantiate `Lock`, `LockStorage`, or any
/// executor type directly.
///
/// - When `conditional` is `Some(...)` and `supports_cas` is `true`, the
///   engine constructs a [`CasExecutor`]. Otherwise it falls back to a
///   [`LockedExecutor`]. The caller is responsible for probing conditional
///   capabilities (via [`crate::probe::probe_conditional_capabilities`]) and
///   setting `supports_cas` accordingly before calling this function.
/// - For [`LockStrategy::S3`] the caller must provide `s3_lock_store` (a
///   [`ConditionalStore`] tuned for short-lived lock requests; subsystems
///   build one for their data store and can wrap the lock-tuned client the
///   same way).
///
/// Returns the constructed `Arc<dyn TransactionExecutor>`. Callers that need
/// the lock primitive or conditional store (for example, to wire the
/// recovery loop and lock janitor) fetch them through
/// [`TransactionExecutor::lock`] and
/// [`TransactionExecutor::conditional_store`].
///
/// # Errors
///
/// Returns [`Error::Build`] when `LockStrategy::S3` is selected without an
/// `s3_lock_store`, when the `redis` feature is not enabled and Redis is
/// selected, or when the underlying lock or executor builder rejects its
/// inputs.
pub fn build_executor(
    store: Arc<dyn ObjectStore>,
    conditional: Option<Arc<dyn ConditionalStore>>,
    lock_strategy: LockStrategy,
    s3_lock_store: Option<Arc<dyn ConditionalStore>>,
    s3_lock_delete_if_match: bool,
    supports_cas: bool,
) -> Result<Arc<dyn TransactionExecutor>, Error> {
    // Capture a stable label for the lock-object backend before the match
    // below moves `lock_strategy`. Logged alongside the executor choice so
    // operators are not misled into reading the lock strategy as the
    // coordination path: both executors share this backend.
    let lock_backend = match &lock_strategy {
        LockStrategy::Memory => "memory",
        #[cfg(feature = "redis")]
        LockStrategy::Redis(_) => "redis",
        LockStrategy::S3(_) => "s3",
    };

    // Each arm yields the lock-object storage plus a `LockBuilder` primed with
    // the per-strategy tuning carried in the strategy config. The tuning is
    // threaded directly into the builder (never stored as a Config field), and
    // the Lock is built once after the match.
    let lock_builder = match lock_strategy {
        LockStrategy::Memory => {
            let storage: Arc<dyn LockStorage> = Arc::new(MemoryLockStorage::new());
            // Memory backend: keep builder defaults.
            Lock::builder(storage)
        }
        #[cfg(feature = "redis")]
        LockStrategy::Redis(config) => {
            let storage: Arc<dyn LockStorage> =
                Arc::new(RedisLockStorage::new(&config).map_err(|e| {
                    Error::Build(format!("failed to build Redis lock storage: {e}"))
                })?);
            // Redis TTL is enforced natively by the storage; only the retry
            // tuning is threaded into the lock primitive.
            Lock::builder(storage)
                .max_retries(config.max_retries)
                .retry_delay_ms(config.retry_delay_ms)
        }
        LockStrategy::S3(config) => {
            let lock_store = s3_lock_store.ok_or_else(|| {
                Error::Build("S3 lock strategy requires an S3 conditional store".to_string())
            })?;
            let storage: Arc<dyn LockStorage> =
                Arc::new(S3LockStorage::new(lock_store, s3_lock_delete_if_match));
            Lock::builder(storage)
                .ttl_secs(config.ttl_secs)
                .max_retries(config.max_retries)
                .retry_delay_ms(config.retry_delay_ms)
                .max_hold_secs(config.max_hold_secs)
        }
    };

    let lock = Arc::new(
        lock_builder
            .build()
            .map_err(|e| Error::Build(format!("failed to build lock: {e}")))?,
    );

    if supports_cas && let Some(cs) = conditional {
        let exec = CasExecutor::builder(cs, lock).build();
        info!(
            executor = "cas",
            lock_backend, "transactional engine executor selected"
        );
        return Ok(Arc::new(exec));
    }

    let exec = LockedExecutor::builder(store, lock).build();
    info!(
        executor = "locked",
        lock_backend, "transactional engine executor selected"
    );
    Ok(Arc::new(exec))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use bytes::Bytes;

    use angos_storage::MemoryObjectStore;

    use super::*;
    use crate::transaction::Mutation;

    fn memory_executor(backend: Arc<MemoryObjectStore>) -> LockedExecutor {
        let lock = Arc::new(
            Lock::builder(Arc::new(MemoryLockStorage::new()))
                .build()
                .expect("test lock"),
        );
        LockedExecutor::builder(backend, lock).build()
    }

    fn put_tx(key: &str) -> Transaction {
        Transaction::builder()
            .mutation(Mutation::Put {
                key: key.to_string(),
                body: Bytes::from_static(b"v"),
                expected: None,
            })
            .build()
    }

    #[tokio::test]
    async fn build_conflict_is_retried_with_fresh_state() {
        let backend = Arc::new(MemoryObjectStore::new());
        let executor = memory_executor(backend);
        let calls = AtomicU32::new(0);

        let outcome = execute_with_retry(
            &executor,
            || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err(Error::Conflict);
                }
                Ok(put_tx("k"))
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await;

        assert!(outcome.is_ok(), "conflicting builds must be retried");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn persistent_build_conflict_exhausts_the_budget() {
        let backend = Arc::new(MemoryObjectStore::new());
        let executor = memory_executor(backend);
        let calls = AtomicU32::new(0);

        let result = execute_with_retry_payload(
            &executor,
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(Transaction, ()), Error>(Error::Conflict)
            },
            2,
        )
        .await;

        assert!(matches!(result, Err(Error::Conflict)));
        assert_eq!(calls.load(Ordering::SeqCst), 3, "initial attempt plus two retries");
    }

    #[tokio::test]
    async fn non_retriable_build_error_propagates_immediately() {
        let backend = Arc::new(MemoryObjectStore::new());
        let executor = memory_executor(backend);
        let calls = AtomicU32::new(0);

        let result = execute_with_retry(
            &executor,
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<Transaction, Error>(Error::Build("bad plan".to_string()))
            },
            DEFAULT_RETRY_BUDGET,
        )
        .await;

        assert!(matches!(result, Err(Error::Build(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
