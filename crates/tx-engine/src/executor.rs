//! Transaction executor trait and outcome type.

pub mod cas;
pub mod common;
pub mod locked;

use std::future::Future;

use async_trait::async_trait;
use tracing::debug;
use uuid::Uuid;

use crate::{error::Error, lock::LockSession, transaction::Transaction};

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
}

/// Execute a transaction and a caller-defined payload built by `build`,
/// retrying on [`Error::Conflict`] or [`Error::Precondition`] up to
/// `max_attempts` additional times.
///
/// `build` is called once before each attempt so the transaction can
/// incorporate fresh state on every retry. Any error returned by `build`
/// is propagated immediately without retrying.
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
        let (tx, payload) = build().await?;
        match executor.execute(tx).await {
            Ok(o) => return Ok((o, payload)),
            Err(Error::Conflict | Error::Precondition) if attempts < max_attempts => {
                debug!(attempts, max_attempts, "Transaction conflict, retrying");
                attempts += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Execute a transaction built by `build`, retrying on [`Error::Conflict`]
/// or [`Error::Precondition`] up to `max_attempts` additional times.
///
/// `build` is called once before each attempt so the transaction can
/// incorporate fresh state on every retry. Any error returned by `build`
/// is propagated immediately without retrying.
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
    let (outcome, ()) = execute_with_retry_payload(
        executor,
        move || {
            let fut = build();
            async move { fut.await.map(|tx| (tx, ())) }
        },
        max_attempts,
    )
    .await?;
    Ok(outcome)
}
