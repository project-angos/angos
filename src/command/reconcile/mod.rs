//! `angos reconcile`: on-demand passes that bring stored state in line with
//! the configuration, each enqueueing jobs rather than acting inline so it
//! gets the event path's retry, backoff and coalescing.

pub mod index;
pub mod replication;
pub mod scan;

use argh::FromArgs;

use crate::{command::maintenance::Error, configuration::Configuration};

#[derive(FromArgs, PartialEq, Debug)]
#[argh(
    subcommand,
    name = "reconcile",
    description = "Reconcile stored content with the configuration"
)]
pub struct Options {
    #[argh(subcommand)]
    pub target: Target,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
pub enum Target {
    Replication(replication::Options),
    Scan(scan::Options),
    Index(index::Options),
}

pub async fn run(options: &Options, config: &Configuration) -> Result<(), Error> {
    match &options.target {
        Target::Replication(options) => replication::run(options, config).await,
        Target::Scan(options) => scan::run(options, config).await,
        Target::Index(options) => index::run(options, config).await,
    }
}
