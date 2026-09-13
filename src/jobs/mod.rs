//! The job-queue subsystem: [`store`] persists and claims durable jobs over the
//! shared object store, [`runner`] drives one claimed job through its handler,
//! and this module holds the value types the CLI, admin API, and router share.
//!
//! Domain handlers (cache fill, replication push) stay with their domains.

pub mod runner;
pub mod store;

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

/// The durable job queues, selecting the storage prefix, the worker's `--queue`
/// filter, and the dispatch handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Queue {
    Cache,
    Replication,
    Scan,
    Index,
}

impl Queue {
    /// The on-disk / metric-label name (`"cache"`, `"replication"`, `"scan"`
    /// or `"index"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Queue::Cache => "cache",
            Queue::Replication => "replication",
            Queue::Scan => "scan",
            Queue::Index => "index",
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
            "scan" => Ok(Queue::Scan),
            "index" => Ok(Queue::Index),
            other => Err(format!(
                "unknown queue '{other}'; expected 'cache', 'replication', 'scan' or 'index'"
            )),
        }
    }
}

/// Which durable partition a job lives in, addressing admin mutations at the
/// correct storage prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Failed,
}
