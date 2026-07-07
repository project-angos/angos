//! Job-queue vocabulary shared across altitudes: the durable job store, the
//! CLI commands, the admin API actions, and the request router all speak these
//! serialized value types without depending on the queue machinery itself.
//!
//! The machinery itself lives beneath: [`store`] persists and claims durable
//! jobs over the storage façade, and [`runner`] drives one claimed job
//! through its handler. Domain handlers (cache fill, replication push) stay
//! with their domains; only the queue subsystem lives here.

pub mod runner;
pub mod store;

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

/// The set of durable job queues. Selects the storage prefix, the worker's
/// `--queue` filter, and the dispatch handler. Serializes as its lowercase name
/// so stored envelopes, storage paths, and metric labels keep their string form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Queue {
    Cache,
    Replication,
}

impl Queue {
    /// The on-disk / metric-label name (`"cache"` or `"replication"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Queue::Cache => "cache",
            Queue::Replication => "replication",
        }
    }
}

impl Display for Queue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Queue {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cache" => Ok(Queue::Cache),
            "replication" => Ok(Queue::Replication),
            other => Err(format!(
                "unknown queue '{other}'; expected 'cache' or 'replication'"
            )),
        }
    }
}

/// Which durable partition a job lives in, used to address admin mutations
/// (`retry`/`delete`) at the correct storage prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Failed,
}
