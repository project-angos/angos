//! Transaction value type: a declarative description of reads and mutations
//! that the engine either commits atomically or leaves entirely unapplied.

use bytes::Bytes;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use angos_storage::Etag;

/// A fingerprint used to detect concurrent modification of a key.
///
/// Engine-internal type: a 32-byte SHA-256 content hash derived from the body
/// the caller observed. Both executors use it the same way: the Locked
/// executor re-reads and re-hashes under the lock; the CAS executor re-reads
/// and re-hashes at Prepare time.
pub type Fingerprint = [u8; 32];

/// A single key read that the transaction depends on.
///
/// If the key's state differs from `fingerprint` at Prepare time, the
/// executor aborts the transaction with a `Conflict` error and the caller
/// retries with a fresh read.
#[derive(Clone, Debug)]
pub struct Read {
    /// The storage key to observe.
    pub key: String,
    /// The expected fingerprint at commit time. Engine-internal; callers
    /// supply body bytes via [`TransactionBuilder::read`] and the fingerprint
    /// is derived automatically.
    pub fingerprint: Fingerprint,
}

impl Read {
    /// Whether this read recorded the key as absent (the fingerprint of an
    /// empty body, per [`TransactionBuilder::read`]); a missing key at verify
    /// time then matches instead of conflicting.
    #[must_use]
    pub fn expects_absent(&self) -> bool {
        let empty: Fingerprint = Sha256::digest([]).into();
        self.fingerprint == empty
    }
}

/// A single mutation to be applied atomically.
///
/// The executor drives each variant with the appropriate storage primitive
/// (unconditional `put`/`delete` under a lock, or `put_if_match`/`delete_if_match`
/// via CAS, depending on the chosen executor).
#[derive(Clone, Debug)]
pub enum Mutation {
    /// Write `body` to `key`, replacing any existing object.
    ///
    /// When `expected` is `Some`, both executors verify the current etag
    /// matches before writing: the CAS executor via `put_if_match`, and the
    /// Locked executor via a HEAD + `ETag` comparison under the lock.
    Put {
        key: String,
        body: Bytes,
        expected: Option<Etag>,
    },

    /// Write `body` to `key` only if the key does not yet exist.
    ///
    /// On the CAS executor this maps to `put_if_absent`. On the Locked
    /// executor it is emulated with a `head` + conditional `put` under the
    /// key's lock.
    PutIfAbsent { key: String, body: Bytes },

    /// Delete `key`.
    ///
    /// When `expected` is `Some`, both executors prevent deleting an object
    /// whose etag has changed since the transaction was built: the CAS executor
    /// uses `delete_if_match`, and the Locked executor performs a HEAD + `ETag`
    /// comparison under the key's lock (a missing key is treated as a no-op
    /// success in both cases).
    Delete { key: String, expected: Option<Etag> },

    /// Server-side copy from `src` to `dst`.
    ///
    /// The engine calls `ObjectStore::copy`. Neither end of the copy is
    /// held in `.tx-bodies`; this is intended for promoting staged data
    /// (already in the store) to its canonical location.
    Copy { src: String, dst: String },

    /// Server-side move from `src` to `dst`: `copy(src, dst)` followed by
    /// `delete(src)`.
    ///
    /// Both steps are individually idempotent under replay: a `delete` of a
    /// missing `src` is treated as success, and `copy` is overwrite-anywhere.
    Move { src: String, dst: String },

    /// Idempotently merge `add`/`remove` into the JSON-array set stored at
    /// `key`.
    ///
    /// The object is a JSON array treated as a set whose members compare by
    /// structural JSON equality. Apply removes every member of `remove`, inserts
    /// every member of `add` not already present, and deletes `key` when the set
    /// becomes empty. Unlike a `Put`, it carries no build-time etag: each apply
    /// (and every recovery replay) re-reads and recomputes against live state,
    /// so a lost race is retried rather than left as a permanent partial commit.
    /// `add` and `remove` must be disjoint, which makes the merge commutative.
    MergeSet {
        key: String,
        add: Vec<Value>,
        remove: Vec<Value>,
    },
}

impl Mutation {
    /// Return the destination key that this mutation writes to or deletes.
    ///
    /// For `Copy` and `Move`, this is the destination key (`dst`).
    pub fn key(&self) -> &str {
        match self {
            Mutation::Put { key, .. }
            | Mutation::PutIfAbsent { key, .. }
            | Mutation::Delete { key, .. }
            | Mutation::MergeSet { key, .. } => key,
            Mutation::Copy { dst, .. } | Mutation::Move { dst, .. } => dst,
        }
    }

    /// Return all keys this mutation touches (both source and destination for
    /// `Copy`/`Move`, so they can be included in the lock set).
    pub fn all_keys(&self) -> impl Iterator<Item = &str> {
        match self {
            Mutation::Copy { src, dst } | Mutation::Move { src, dst } => {
                vec![src.as_str(), dst.as_str()].into_iter()
            }
            _ => vec![self.key()].into_iter(),
        }
    }
}

/// A declarative description of a transaction.
///
/// Built via the builder returned by [`Transaction::builder`].  Callers
/// assemble reads and mutations, then hand the completed value to a
/// [`TransactionExecutor`](crate::executor::TransactionExecutor).
///
/// The engine never modifies a `Transaction` in place; it is consumed by the
/// executor.
#[derive(Clone, Debug)]
pub struct Transaction {
    /// Keys whose state the transaction depends on. If any fingerprint
    /// mismatches at Prepare, the transaction is aborted.
    pub reads: Vec<Read>,
    /// Mutations to apply atomically.
    pub mutations: Vec<Mutation>,
    /// Additional keys to serialise on that are neither read nor written.
    ///
    /// Used to close races against subsystems that touch a shared resource
    /// outside the transaction's read/mutation set (e.g. `blob-data:{digest}`
    /// while a manifest delete's link transaction is in flight).
    pub coarse_lock_keys: Vec<String>,
}

/// Collect an iterator of lock keys into a sorted, de-duplicated set.
///
/// This is the single authoritative "shape" for every lock-set derivation
/// (reads ∪ mutation keys ∪ coarse lock keys). Each caller builds its own key
/// iterator (the families differ: [`Transaction`]/[`Read`]/[`Mutation`] here,
/// the `IntentRecord`/`ReadRecord`/`MutationRecord` family in recovery), then
/// passes it here so the result stays byte-identical across call sites.
#[must_use]
pub fn lock_key_set(keys: impl Iterator<Item = String>) -> Vec<String> {
    let mut keys: Vec<String> = keys.collect();
    keys.sort();
    keys.dedup();
    keys
}

impl Transaction {
    /// Return a builder for constructing a `Transaction`.
    #[must_use]
    pub fn builder() -> TransactionBuilder {
        TransactionBuilder::new()
    }

    /// Construct a `Transaction` from pre-computed `reads` and `mutations`.
    ///
    /// Use this when the caller already has [`Read`] fingerprints captured
    /// from a prior read (e.g. via a planner that exposes its mutation set)
    /// and would otherwise have to re-hash the bodies to feed the builder.
    /// Coarse lock keys default to empty; add via direct field access if
    /// needed.
    #[must_use]
    pub fn from_parts(reads: Vec<Read>, mutations: Vec<Mutation>) -> Self {
        Self {
            reads,
            mutations,
            coarse_lock_keys: Vec::new(),
        }
    }

    /// Collect the full set of keys that must be locked for this transaction
    /// (reads ∪ mutations ∪ coarse lock keys), sorted and de-duplicated.
    #[must_use]
    pub fn lock_set(&self) -> Vec<String> {
        lock_key_set(
            self.reads
                .iter()
                .map(|r| r.key.clone())
                .chain(
                    self.mutations
                        .iter()
                        .flat_map(|m| m.all_keys().map(ToOwned::to_owned)),
                )
                .chain(self.coarse_lock_keys.iter().cloned()),
        )
    }
}

/// Builder for [`Transaction`].
///
/// Constructed via [`Transaction::builder`].
#[derive(Default)]
pub struct TransactionBuilder {
    reads: Vec<Read>,
    mutations: Vec<Mutation>,
    coarse_lock_keys: Vec<String>,
}

impl TransactionBuilder {
    /// Create a new, empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a read dependency.
    ///
    /// `body` is the raw bytes the caller observed when reading `key`. The
    /// engine computes a SHA-256 fingerprint from those bytes and uses it to
    /// detect concurrent modifications: under the Locked executor the hash is
    /// re-verified after the lock is acquired; under the CAS executor it is
    /// re-verified at Prepare time.
    ///
    /// Passing an empty slice records the key as absent; any subsequent write
    /// to that key before Apply will be detected as a conflict.
    #[must_use]
    pub fn read(mut self, key: impl Into<String>, body: impl Into<Bytes>) -> Self {
        let hash: [u8; 32] = Sha256::digest(body.into()).into();
        self.reads.push(Read {
            key: key.into(),
            fingerprint: hash,
        });
        self
    }

    #[must_use]
    pub fn mutation(mut self, m: Mutation) -> Self {
        self.mutations.push(m);
        self
    }

    /// Add a coarse lock key.
    ///
    /// The key is folded into the transaction's lock set but is otherwise
    /// not read or written. Use for serialising against subsystems that
    /// touch a shared resource outside the transaction's working set.
    #[must_use]
    pub fn coarse_lock(mut self, key: impl Into<String>) -> Self {
        self.coarse_lock_keys.push(key.into());
        self
    }

    /// Consume the builder and produce the [`Transaction`].
    #[must_use]
    pub fn build(self) -> Transaction {
        Transaction {
            reads: self.reads,
            mutations: self.mutations,
            coarse_lock_keys: self.coarse_lock_keys,
        }
    }
}
