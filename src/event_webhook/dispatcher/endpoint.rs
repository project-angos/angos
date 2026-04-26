use std::sync::Arc;

use reqwest::Client;

use crate::event_webhook::{config::EventWebhookConfig, event::EventKind};

pub struct WebhookEndpoint {
    pub client: Client,
    pub config: Arc<EventWebhookConfig>,
}

impl WebhookEndpoint {
    pub fn matches_event(&self, event_kind: &EventKind, repository: &str) -> bool {
        if !self.config.events.contains(event_kind) {
            return false;
        }
        match &self.config.repository_filter {
            None => true,
            Some(filters) => filters.iter().any(|p| p.is_match(repository)),
        }
    }
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
        Some(filters) => filters.iter().any(|p| p.is_match(repository)),
    }
}
