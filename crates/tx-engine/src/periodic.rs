//! Shared scaffold for the engine's periodic background loops.
//!
//! The recovery loop and the body/lock janitors all run the same shape: an
//! [`interval`] ticker with [`MissedTickBehavior::Skip`], a `biased` `select!`
//! that returns immediately on cancellation, and a sweep body invoked on every
//! tick. [`run_periodic`] owns that scaffold so each consumer supplies only its
//! interval, cancellation token, a label for the cancellation log, and the
//! per-tick sweep closure.

use std::future::Future;
use std::time::Duration;

use tokio::{
    select,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Drive a periodic sweep until `cancellation` fires.
///
/// Ticks an [`interval`] timer (with [`MissedTickBehavior::Skip`], so a slow
/// sweep coalesces missed ticks rather than bursting) and runs `sweep` once per
/// tick. The `select!` is `biased` so a pending cancellation always wins over a
/// ready tick: on cancellation the loop logs `"<label>: cancellation received,
/// stopping"` at debug and returns.
pub async fn run_periodic<F, Fut>(
    period: Duration,
    cancellation: &CancellationToken,
    label: &str,
    mut sweep: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        select! {
            biased;
            () = cancellation.cancelled() => {
                debug!("{label}: cancellation received, stopping");
                return;
            }
            _ = ticker.tick() => {
                sweep().await;
            }
        }
    }
}
