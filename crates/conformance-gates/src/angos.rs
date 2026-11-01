use std::env;
use std::fmt;
use std::fs::write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;

use tokio::process::Command;
use tokio::time::sleep;

use crate::error::{GateError, GateResult, ensure};

/// Delay step between chaos rounds: round N gets killed N steps in, so the
/// walk dies at a different point every time.
const CHAOS_KILL_STEP: Duration = Duration::from_millis(50);
const SUMMARY_MARKER: &str = "scrub complete: walked";

/// Runs the angos CLI under test, either as a local binary
/// (`GATE_ANGOS_BIN` + `GATE_CONFIG`) or a container
/// (`GATE_ANGOS_DOCKER_IMAGE` + `GATE_CONFIG`, the CI mode).
pub struct AngosRunner {
    mode: RunnerMode,
    config: String,
}

enum RunnerMode {
    Binary(String),
    Docker {
        image: String,
        conformance_dir: PathBuf,
        user: Option<String>,
    },
}

impl AngosRunner {
    /// Construct from the environment contract; the docker image takes
    /// precedence when both run modes are configured.
    pub fn from_env() -> GateResult<Self> {
        let config = env::var("GATE_CONFIG")
            .map_err(|_| GateError::Environment("GATE_CONFIG must be set".into()))?;
        let mode = if let Ok(image) = env::var("GATE_ANGOS_DOCKER_IMAGE") {
            // The container sees the fs store and configs under /conformance,
            // exactly like the CI backend container it shares the store with.
            let conformance_dir = env::current_dir()?.join("conformance");
            RunnerMode::Docker {
                image,
                conformance_dir,
                user: env::var("GATE_DOCKER_USER").ok().filter(|u| !u.is_empty()),
            }
        } else if let Ok(bin) = env::var("GATE_ANGOS_BIN") {
            RunnerMode::Binary(bin)
        } else {
            return Err(GateError::Environment(
                "one of GATE_ANGOS_BIN or GATE_ANGOS_DOCKER_IMAGE must be set".into(),
            ));
        };
        Ok(Self { mode, config })
    }

    /// The config file every run uses unless overridden.
    pub fn config_path(&self) -> &str {
        &self.config
    }

    /// Run an angos subcommand to completion, writing its combined output to
    /// `log_path` (also on failure, for the CI artifact) and returning it.
    pub async fn run_logged(&self, args: &[&str], log_path: &Path) -> GateResult<String> {
        self.run_with_config(&self.config, args, log_path).await
    }

    /// Like [`Self::run_logged`] but with an alternate config file, for gates
    /// that need configuration the store under test does not carry (e.g. a
    /// retention policy). Must be an absolute path in docker mode.
    pub async fn run_with_config(
        &self,
        config: &str,
        args: &[&str],
        log_path: &Path,
    ) -> GateResult<String> {
        let output = self
            .command_with(config, args)
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| {
                GateError::Failure(format!("failed to spawn angos {}: {e}", args.join(" ")))
            })?;
        let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
        combined.push_str(&String::from_utf8_lossy(&output.stderr));
        write(log_path, &combined)?;
        ensure(output.status.success(), || {
            let mut tail: Vec<&str> = combined.lines().rev().take(20).collect();
            tail.reverse();
            format!(
                "angos {} exited with {}; log tail:\n{}",
                args.join(" "),
                output.status,
                tail.join("\n")
            )
        })?;
        Ok(combined)
    }

    /// Chaos round: start a scrub and SIGKILL it mid-run, `round` delay steps
    /// in. Both run modes discard output; the kill must land while the walk
    /// is still going.
    pub async fn kill_mid_scrub(&self, round: u32) -> GateResult<()> {
        let delay = CHAOS_KILL_STEP * round;
        match &self.mode {
            RunnerMode::Binary(_) => {
                let mut child = self
                    .command_with(&self.config, &["scrub"])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?;
                sleep(delay).await;
                child.kill().await?;
            }
            RunnerMode::Docker {
                image,
                conformance_dir,
                user,
            } => {
                let name = format!("gate-chaos-{round}");
                let start = docker_run(
                    &["-d", "--name", &name],
                    user.as_deref(),
                    &self.config,
                    conformance_dir,
                    image,
                    &["scrub"],
                );
                run_quiet(start).await?;
                sleep(delay).await;
                // The container may finish before the kill lands; both
                // commands tolerate an already-gone target.
                let _ = run_quiet(docker_command(&["kill", &name])).await;
                let _ = run_quiet(docker_command(&["rm", "-f", &name])).await;
            }
        }
        Ok(())
    }

    fn command_with(&self, config: &str, args: &[&str]) -> Command {
        match &self.mode {
            RunnerMode::Binary(bin) => {
                let mut cmd = Command::new(bin);
                cmd.env("RUST_LOG", "info").arg("-c").arg(config);
                cmd.args(args);
                cmd
            }
            RunnerMode::Docker {
                image,
                conformance_dir,
                user,
            } => docker_run(
                &["--rm"],
                user.as_deref(),
                config,
                conformance_dir,
                image,
                args,
            ),
        }
    }
}

/// Build a `docker run` command: `head` flags (detached or `--rm`), an
/// optional `--user`, then the config and conformance mounts, host
/// networking, the image, and its args.
fn docker_run(
    head: &[&str],
    user: Option<&str>,
    config: &str,
    conformance_dir: &Path,
    image: &str,
    args: &[&str],
) -> Command {
    let mut cmd = docker_command(&["run"]);
    cmd.args(head);
    if let Some(user) = user {
        cmd.args(["--user", user]);
    }
    cmd.args(["-e", "RUST_LOG=info"]);
    cmd.arg("-v").arg(format!("{config}:/config.toml"));
    cmd.arg("-v")
        .arg(format!("{}:/conformance", conformance_dir.display()));
    cmd.args(["--network", "host", image]);
    cmd.args(args);
    cmd
}

/// The five counters of a scrub run's summary line.
pub struct ScrubSummary {
    pub walked: u64,
    pub repairs: u64,
    pub quarantined: u64,
    pub corrupt: u64,
    pub failures: u64,
}

impl FromStr for ScrubSummary {
    type Err = GateError;

    /// Parse the last summary line from a run's combined output. Counting
    /// starts after the marker so tracing timestamps never pollute the parse.
    fn from_str(output: &str) -> GateResult<Self> {
        let idx = output
            .rfind(SUMMARY_MARKER)
            .ok_or_else(|| GateError::Failure("no scrub summary in output".into()))?;
        let line = output[idx..].lines().next().unwrap_or_default();
        let counters: Vec<u64> = line
            .split(|ch: char| !ch.is_ascii_digit())
            .filter_map(|token| token.parse().ok())
            .collect();
        ensure(counters.len() == 5, || {
            format!("malformed scrub summary line: {line}")
        })?;
        Ok(Self {
            walked: counters[0],
            repairs: counters[1],
            quarantined: counters[2],
            corrupt: counters[3],
            failures: counters[4],
        })
    }
}

impl ScrubSummary {
    /// Whether the run proposed or performed no action at all.
    pub fn is_all_zero(&self) -> bool {
        let total = self.repairs + self.quarantined + self.corrupt + self.failures;
        total == 0
    }
}

impl fmt::Display for ScrubSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "scrub complete: walked {} key(s): {} repair(s), {} quarantined, {} corrupt deleted, {} failure(s)",
            self.walked, self.repairs, self.quarantined, self.corrupt, self.failures
        )
    }
}

fn docker_command(args: &[&str]) -> Command {
    let mut cmd = Command::new("docker");
    cmd.args(args);
    cmd
}

async fn run_quiet(mut cmd: Command) -> GateResult<()> {
    let status = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    ensure(status.success(), || format!("command exited with {status}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reads_the_last_summary_and_ignores_timestamps() {
        let output = "2026-11-01T10:00:00Z scrub complete: walked 9, 9 repairs, 9 quarantined, 9 corrupt, 9 failures\n\
            2026-11-01T10:00:01Z scrub complete: walked 12, 1 repairs, 0 quarantined, 2 corrupt, 3 failures\n\
            trailing log line 42\n";
        let summary: ScrubSummary = output.parse().unwrap();
        assert_eq!(
            [
                summary.walked,
                summary.repairs,
                summary.quarantined,
                summary.corrupt,
                summary.failures
            ],
            [12, 1, 0, 2, 3]
        );
        assert!("no marker here".parse::<ScrubSummary>().is_err());
    }
}
