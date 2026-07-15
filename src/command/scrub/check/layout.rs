use async_trait::async_trait;
use tracing::debug;

use crate::command::scrub::{
    action::Action, check::StoreChecker, error::Error, executor::ActionSink,
};

/// Migrates metadata layout documents that scrub can discover safely.
///
/// Backend-specific migration details live behind `MetadataStore`; this checker
/// only enumerates registry-wide subjects and emits ordinary scrub actions so
/// dry-run and real runs keep the same control flow as consistency repairs.
#[derive(Default)]
pub struct LayoutChecker;

impl LayoutChecker {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl StoreChecker for LayoutChecker {
    async fn check_all(&self, sink: &mut (dyn ActionSink + Send)) -> Result<(), Error> {
        debug!("Migrating metadata storage layout");

        sink.apply(Action::PruneLegacyNamespaceRegistry).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn layout_checker_emits_prune_legacy_namespace_registry() {
        let checker = LayoutChecker::new();
        let mut actions = Vec::new();
        checker.check_all(&mut actions).await.unwrap();

        assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::PruneLegacyNamespaceRegistry))
        );
    }
}
