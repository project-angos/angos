//! The scrub gate harness. Every check here is ground truth: store snapshots
//! are diffed byte-level instead of trusting scrub's own summary, image
//! content is re-hashed against its declared digests, and repairs must stay
//! inside the seeded blast radius.
//!
//! Environment contract (shared by all gates):
//!   `GATE_MODE=fs|s3`
//!   `GATE_DATA_ROOT`              (fs) store root directory
//!   `GATE_ENDPOINT` `GATE_BUCKET` (s3)
//!   `GATE_ANGOS_BIN` `GATE_CONFIG`         run angos as a local binary, or
//!   `GATE_ANGOS_DOCKER_IMAGE` `GATE_CONFIG`  as a container (CI)
//!   `STATE_DIR`                   logs and snapshots for failure forensics
//!   `REGISTRY_URL`                default `http://127.0.0.1:8000`

mod angos;
mod error;
mod gates;
mod registry;
mod seed;
mod store;

use std::env;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::process::{ExitCode, id};

use argh::FromArgs;

use crate::angos::AngosRunner;
use crate::error::GateResult;
use crate::gates::{DEFAULT_POPULATE_IMAGES, GateContext};
use crate::registry::RegistryClient;
use crate::store::GateStore;

#[derive(FromArgs)]
/// Ground-truth gates challenging angos scrub and prune against a live store.
struct Cli {
    #[argh(subcommand)]
    command: Gate,
}

#[derive(FromArgs)]
#[argh(subcommand)]
enum Gate {
    Healthy(HealthyGate),
    Corruption(CorruptionGate),
    Chaos(ChaosGate),
    Populate(PopulateCmd),
}

#[derive(FromArgs)]
/// Prove a healthy store is left byte-identical: dry-run purity, zero-action
/// floors, prune sweep proofs, full-catalog digest audit.
#[argh(subcommand, name = "healthy")]
struct HealthyGate {}

#[derive(FromArgs)]
/// Seed the full defect matrix, then pin counters, end states, blast radius,
/// decoy survival, and the prune window.
#[argh(subcommand, name = "corruption")]
struct CorruptionGate {}

#[derive(FromArgs)]
/// SIGKILL scrub mid-run three times, then require convergence and integrity.
#[argh(subcommand, name = "chaos")]
struct ChaosGate {}

#[derive(FromArgs)]
/// Push synthetic images so a fresh local store clears the gate floors.
#[argh(subcommand, name = "populate")]
struct PopulateCmd {
    /// how many images to push (default 60)
    #[argh(option, default = "DEFAULT_POPULATE_IMAGES")]
    images: usize,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli: Cli = argh::from_env();
    match run(cli.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(gate: Gate) -> GateResult<()> {
    let ctx = GateContext {
        store: GateStore::from_env()?,
        registry: RegistryClient::from_env(),
        runner: AngosRunner::from_env()?,
        state_dir: state_dir()?,
    };
    match gate {
        Gate::Healthy(_) => gates::healthy(&ctx).await,
        Gate::Corruption(_) => gates::corruption(&ctx).await,
        Gate::Chaos(_) => gates::chaos(&ctx).await,
        Gate::Populate(cmd) => gates::populate(&ctx, cmd.images).await,
    }
}

/// `STATE_DIR` from the environment, or a per-process temp directory. It is
/// created here so gates can write forensics unconditionally.
fn state_dir() -> GateResult<PathBuf> {
    let dir = match env::var("STATE_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => env::temp_dir().join(format!("scrub-gates-{}", id())),
    };
    create_dir_all(&dir)?;
    Ok(dir)
}
