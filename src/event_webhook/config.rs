use hyper::Uri;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::{configuration::Error, event_webhook::event::EventKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryPolicy {
    Required,
    Optional,
    Async,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventWebhookConfig {
    pub url: String,
    pub policy: DeliveryPolicy,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub max_retries: u32,
    pub events: Vec<EventKind>,
    #[serde(default)]
    pub repository_filter: Option<Vec<String>>,
}

fn default_timeout_ms() -> u64 {
    5000
}

impl EventWebhookConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if let Err(e) = Uri::try_from(&self.url) {
            return Err(Error::Initialization(format!(
                "Invalid event webhook URL: {e}"
            )));
        }

        if self.events.is_empty() {
            return Err(Error::Initialization(
                "Event webhook must have at least one event".to_string(),
            ));
        }

        if let Some(filters) = &self.repository_filter {
            for pattern in filters {
                if let Err(e) = Regex::new(pattern) {
                    return Err(Error::Initialization(format!(
                        "Invalid repository filter regex '{pattern}': {e}"
                    )));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::event_webhook::{
        config::{DeliveryPolicy, EventWebhookConfig},
        event::EventKind,
    };

    #[test]
    fn serialize_delivery_policy_required() {
        let policy = DeliveryPolicy::Required;
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, r#""required""#);
    }

    #[test]
    fn serialize_delivery_policy_optional() {
        let policy = DeliveryPolicy::Optional;
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, r#""optional""#);
    }

    #[test]
    fn serialize_delivery_policy_async() {
        let policy = DeliveryPolicy::Async;
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, r#""async""#);
    }

    #[test]
    fn deserialize_delivery_policy_round_trip() {
        let variants = [
            DeliveryPolicy::Required,
            DeliveryPolicy::Optional,
            DeliveryPolicy::Async,
        ];

        for policy in variants {
            let json = serde_json::to_string(&policy).unwrap();
            let deserialized: DeliveryPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(policy, deserialized);
        }
    }

    #[test]
    fn deserialize_webhook_config_all_fields() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            token = "secret-token"
            timeout_ms = 10000
            max_retries = 3
            events = ["manifest.push", "tag.create"]
            repository_filter = ["^myapp/.*", "^library/.*"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.url, "https://example.com/webhook");
        assert_eq!(config.policy, DeliveryPolicy::Required);
        assert_eq!(config.token, Some("secret-token".to_string()));
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.events.len(), 2);
        assert_eq!(config.events[0], EventKind::ManifestPush);
        assert_eq!(config.events[1], EventKind::TagCreate);
        assert_eq!(
            config.repository_filter,
            Some(vec!["^myapp/.*".to_string(), "^library/.*".to_string()])
        );
    }

    #[test]
    fn deserialize_webhook_config_with_defaults() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "optional"
            events = ["blob.push"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.url, "https://example.com/webhook");
        assert_eq!(config.policy, DeliveryPolicy::Optional);
        assert_eq!(config.token, None);
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.max_retries, 0);
        assert_eq!(config.events.len(), 1);
        assert_eq!(config.events[0], EventKind::BlobPush);
        assert_eq!(config.repository_filter, None);
    }

    #[test]
    fn deserialize_webhook_config_async_policy() {
        let toml = r#"
            url = "https://example.com/async-hook"
            policy = "async"
            events = ["manifest.push", "manifest.delete", "blob.push", "tag.create", "tag.delete"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.policy, DeliveryPolicy::Async);
        assert_eq!(config.events.len(), 5);
    }

    #[test]
    fn validate_webhook_config_invalid_url_rejected() {
        let toml = r#"
            url = "ht!tp://::invalid"
            policy = "required"
            events = ["manifest.push"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn validate_webhook_config_invalid_regex_rejected() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = ["manifest.push"]
            repository_filter = ["^valid/.*", "[invalid"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn validate_webhook_config_empty_events_rejected() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = []
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn validate_webhook_config_valid_passes() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "optional"
            events = ["manifest.push"]
            repository_filter = ["^myapp/.*"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn validate_webhook_config_valid_without_filter() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "async"
            events = ["tag.create", "tag.delete"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        let result = config.validate();
        assert!(result.is_ok());
    }
}
