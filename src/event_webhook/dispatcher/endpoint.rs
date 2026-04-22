use regex::Regex;
use reqwest::Client;
use tracing::warn;

use crate::event_webhook::{config::EventWebhookConfig, event::EventKind};

pub(super) struct WebhookEndpoint {
    pub(super) client: Client,
    pub(super) config: EventWebhookConfig,
    pub(super) compiled_filters: Vec<Regex>,
}

impl WebhookEndpoint {
    pub(super) fn matches_event(&self, event_kind: &EventKind, repository: &str) -> bool {
        if !self.config.events.contains(event_kind) {
            return false;
        }
        if self.compiled_filters.is_empty() {
            return true;
        }
        self.compiled_filters
            .iter()
            .any(|re| re.is_match(repository))
    }
}

pub(super) fn compile_filters(patterns: &[String], webhook_name: &str) -> Vec<Regex> {
    let mut compiled = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        match Regex::new(pattern) {
            Ok(regex) => compiled.push(regex),
            Err(e) => warn!(
                "Invalid repository_filter regex '{pattern}' for webhook '{webhook_name}': {e}"
            ),
        }
    }
    compiled
}

#[cfg(test)]
pub fn matches_event(
    config: &EventWebhookConfig,
    event_kind: &EventKind,
    repository: &str,
) -> bool {
    if !config.events.contains(event_kind) {
        return false;
    }

    match &config.repository_filter {
        None => true,
        Some(filters) => filters
            .iter()
            .any(|pattern| Regex::new(pattern).is_ok_and(|re| re.is_match(repository))),
    }
}
