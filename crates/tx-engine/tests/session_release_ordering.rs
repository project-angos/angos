//! Tests for the explicit session-release ordering contract.
//!
//! The executor no longer takes a caller session: callers obtain a session
//! through `try_acquire`/`acquire`, hold it across the transaction (or the
//! whole `execute_with_retry` loop), then release it themselves. These tests
//! cover that pattern end-to-end:
//!
//! - The executor never releases a caller session.
//! - A caller holding a session across `execute_with_retry` releases it after
//!   the transaction commits.
//! - The lock object is removed from lock storage after the explicit release.
//! - Release happens after the intent log is reaped (no orphaned .tx-log keys).
//! - The session survives a `Conflict`-exhausted retry loop and can still be
//!   released cleanly.

use std::sync::Arc;

use bytes::Bytes;

use angos_storage::{MemoryObjectStore, ObjectStore};

use angos_tx_engine::{
    error::Error,
    executor::{
        DEFAULT_RETRY_BUDGET, Outcome, TransactionExecutor, execute_with_retry,
        locked::LockedExecutor,
    },
    transaction::{Mutation, Transaction},
};

mod common;

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

fn build_executor(store: Arc<dyn ObjectStore>) -> LockedExecutor {
    LockedExecutor::builder(store, common::memory_lock()).build()
}

fn simple_tx() -> Transaction {
    Transaction::builder()
        .mutation(Mutation::Put {
            key: "output".to_string(),
            body: Bytes::from_static(b"value"),
            expected: None,
        })
        .build()
}

/// Return `true` when `keys` are currently locked, observable by a
/// non-blocking `try_acquire` returning `None`. If the keys are free
/// `try_acquire` returns `Some(session)`; we release it immediately so the
/// probe leaves storage pristine.
async fn keys_are_held(executor: &LockedExecutor, keys: &[String]) -> bool {
    match executor
        .try_acquire(keys)
        .await
        .expect("try_acquire infallible on memory storage")
    {
        None => true,
        Some(s) => {
            s.release().await;
            false
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

/// Hold a session across `execute_with_retry`, then release it explicitly.
/// During the call the lock is held; after the call the executor has not
/// touched it; after explicit release the lock is free.
#[tokio::test]
async fn caller_session_released_after_successful_execute_with_retry() {
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::new());
    let executor = build_executor(store.clone());

    let caller_keys = vec!["job:lock_key:demo".to_string()];

    let session = executor
        .acquire(&caller_keys)
        .await
        .expect("acquire caller session");

    assert!(
        keys_are_held(&executor, &caller_keys).await,
        "caller key should be held by the session before release"
    );

    let result: Result<Outcome, Error> = execute_with_retry(
        &executor,
        || async { Ok(simple_tx()) },
        DEFAULT_RETRY_BUDGET,
    )
    .await;
    assert!(
        result.is_ok(),
        "execute_with_retry should succeed: {result:?}"
    );

    assert!(
        keys_are_held(&executor, &caller_keys).await,
        "executor must not release the caller session"
    );

    session.release().await;
    assert!(
        !keys_are_held(&executor, &caller_keys).await,
        "lock must be free once caller releases the session"
    );
}

/// On a `Conflict`-exhausted retry loop the helper surfaces
/// `Err(Error::Conflict)`; the caller's session is still alive and the lock
/// is still held until the caller explicitly releases it.
#[tokio::test]
async fn caller_session_survives_conflict_exhaust() {
    let mem = MemoryObjectStore::new();
    mem.put("dep", Bytes::from_static(b"body")).await.unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(mem);
    let executor = build_executor(store.clone());

    let caller_keys = vec!["job:lock_key:conflict".to_string()];
    let session = executor
        .acquire(&caller_keys)
        .await
        .expect("acquire caller session");

    let result: Result<Outcome, Error> = execute_with_retry(
        &executor,
        || async {
            Ok(Transaction::builder()
                .read("dep", Bytes::from_static(b"stale-content"))
                .mutation(Mutation::Put {
                    key: "out".to_string(),
                    body: Bytes::from_static(b"x"),
                    expected: None,
                })
                .build())
        },
        2,
    )
    .await;
    assert!(
        matches!(result, Err(Error::Conflict)),
        "exhausted retries must surface Conflict, got: {result:?}"
    );

    assert!(
        !session.cancellation().is_cancelled(),
        "caller session must still be valid after Conflict"
    );
    assert!(
        keys_are_held(&executor, &caller_keys).await,
        "lock must still be held after Conflict exhaust"
    );

    session.release().await;
    assert!(
        !keys_are_held(&executor, &caller_keys).await,
        "explicit release must free the lock"
    );
}

/// `execute_with_retry` returns only after Reap. Once it returns, the intent
/// log and staged bodies are gone — so any explicit release the caller does
/// afterwards is guaranteed to happen after the durable reap.
#[tokio::test]
async fn intent_log_reaped_before_execute_with_retry_returns() {
    let store = Arc::new(MemoryObjectStore::new());
    let store_dyn: Arc<dyn ObjectStore> = store.clone();
    let executor = build_executor(store_dyn);

    let caller_keys = vec!["job:lock_key:reap".to_string()];
    let session = executor
        .acquire(&caller_keys)
        .await
        .expect("acquire caller session");

    let result: Result<Outcome, Error> = execute_with_retry(
        &executor,
        || async { Ok(simple_tx()) },
        DEFAULT_RETRY_BUDGET,
    )
    .await;
    assert!(result.is_ok());

    let stale = store.list(".tx-log/", 100, None).await.unwrap().items;
    assert!(
        stale.is_empty(),
        "intent log must be reaped before execute_with_retry returns: {stale:?}"
    );
    let stale_bodies = store.list(".tx-bodies/", 100, None).await.unwrap().items;
    assert!(
        stale_bodies.is_empty(),
        ".tx-bodies must be reaped before execute_with_retry returns: {stale_bodies:?}"
    );

    session.release().await;
}

/// Explicit release deletes the lock object backing the session: a follow-up
/// `try_acquire` on the same keys succeeds immediately.
#[tokio::test]
async fn explicit_release_deletes_lock_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::new());
    let executor = build_executor(store);

    let caller_keys = vec!["job:lock_key:delete-probe".to_string()];
    let session = executor
        .acquire(&caller_keys)
        .await
        .expect("acquire caller session");

    let result: Result<Outcome, Error> = execute_with_retry(
        &executor,
        || async { Ok(simple_tx()) },
        DEFAULT_RETRY_BUDGET,
    )
    .await;
    assert!(result.is_ok());

    session.release().await;
    let fresh = executor
        .try_acquire(&caller_keys)
        .await
        .expect("try_acquire infallible on memory storage");
    assert!(
        fresh.is_some(),
        "explicit release must remove the lock object so a fresh acquire succeeds"
    );
    if let Some(s) = fresh {
        s.release().await;
    }
}

/// A single `executor.execute(tx)` does not touch the caller-held session:
/// the session remains valid and the lock remains held after the transaction
/// commits.
#[tokio::test]
async fn executor_execute_never_releases_caller_session() {
    let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::new());
    let executor = build_executor(store.clone());

    let caller_keys = vec!["job:lock_key:never-touched".to_string()];
    let session = executor
        .acquire(&caller_keys)
        .await
        .expect("acquire caller session");

    let result: Result<Outcome, Error> = executor.execute(simple_tx()).await;
    assert!(result.is_ok(), "execute should commit: {result:?}");

    assert!(
        !session.cancellation().is_cancelled(),
        "session must still be valid"
    );
    assert!(
        keys_are_held(&executor, &caller_keys).await,
        "executor must not release the caller session"
    );

    session.release().await;
    assert!(
        !keys_are_held(&executor, &caller_keys).await,
        "explicit release frees the lock"
    );
}
