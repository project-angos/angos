use std::sync::Arc;

use reqwest::Client;

use super::transport::DeliveryRequest;
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

    pub fn build_request<'a>(
        &'a self,
        body: &'a [u8],
        event_kind_header: &'a str,
    ) -> DeliveryRequest<'a> {
        DeliveryRequest {
            client: &self.client,
            url: self.config.url.as_str(),
            token: self.config.token.as_ref().map(|t| t.expose().as_str()),
            body,
            event_kind_header,
        }
    }
}
