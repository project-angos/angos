//! Test-only sinks for the maintenance seam.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::command::maintenance::Error;
use crate::command::maintenance::action::Action;
use crate::command::maintenance::executor::ActionSink;

/// Captures actions into a locked `Vec` without performing any I/O, so a test
/// can assert which actions a check produces; the lock lets one capture serve
/// concurrent producers, matching the `&self` sink contract.
#[async_trait]
impl ActionSink for Mutex<Vec<Action>> {
    async fn apply(&self, action: Action) -> Result<(), Error> {
        self.lock()
            .map_err(|e| Error::Initialization(format!("capture sink poisoned: {e}")))?
            .push(action);
        Ok(())
    }
}
