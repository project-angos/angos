use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

use crate::{
    configuration::{Error, RegexPattern},
    event_webhook::event::EventKind,
    secret::Secret,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryPolicy {
    Required,
    Optional,
    Async,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventWebhookConfig {
    /// Populated during configuration resolution; not present in TOML.
    #[serde(skip, default)]
    pub name: String,

    pub url: Url,
    pub policy: DeliveryPolicy,
    #[serde(default)]
    pub token: Option<Secret<String>>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Maximum number of retry attempts after the initial delivery. Accepted range: 0–16.
    /// Values above 16 are rejected at configuration load time to prevent retry storms
    /// and arithmetic overflow in the exponential-backoff calculation.
    #[serde(default, deserialize_with = "deserialize_max_retries")]
    pub max_retries: u32,
    pub events: Vec<EventKind>,
    #[serde(default)]
    pub repository_filter: Option<Vec<RegexPattern>>,
}

fn default_timeout_ms() -> u64 {
    5000
}

fn deserialize_max_retries<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    const MAX: u32 = 16;
    let v = u32::deserialize(deserializer)?;
    if v > MAX {
        return Err(serde::de::Error::custom(format!(
            "max_retries={v} exceeds the supported maximum of {MAX}"
        )));
    }
    Ok(v)
}

impl EventWebhookConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.events.is_empty() {
            return Err(Error::Initialization(
                "Event webhook must have at least one event".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        configuration::RegexPattern,
        event_webhook::{
            config::{DeliveryPolicy, EventWebhookConfig},
            event::EventKind,
        },
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
        assert_eq!(config.url.as_str(), "https://example.com/webhook");
        assert_eq!(config.policy, DeliveryPolicy::Required);
        assert_eq!(
            config.token.as_ref().map(|t| t.expose().as_str()),
            Some("secret-token")
        );
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.events.len(), 2);
        assert_eq!(config.events[0], EventKind::ManifestPush);
        assert_eq!(config.events[1], EventKind::TagCreate);
        assert_eq!(
            config.repository_filter,
            Some(vec![
                RegexPattern::compile("^myapp/.*").unwrap(),
                RegexPattern::compile("^library/.*").unwrap(),
            ])
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
        assert_eq!(config.url.as_str(), "https://example.com/webhook");
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
    fn invalid_url_fails_at_deserialize() {
        let toml = r#"
            url = "ht!tp://::invalid"
            policy = "required"
            events = ["manifest.push"]
        "#;

        let result: Result<EventWebhookConfig, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_repository_filter_fails_at_deserialize() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = ["manifest.push"]
            repository_filter = ["^valid/.*", "[invalid"]
        "#;

        let result: Result<EventWebhookConfig, _> = toml::from_str(toml);
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

    #[test]
    fn max_retries_boundary_accepted() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = ["manifest.push"]
            max_retries = 16
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.max_retries, 16);
    }

    #[test]
    fn max_retries_above_boundary_rejected() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = ["manifest.push"]
            max_retries = 17
        "#;

        let result: Result<EventWebhookConfig, _> = toml::from_str(toml);
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("max_retries=17 exceeds the supported maximum of 16"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn max_retries_far_out_of_range_rejected() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = ["manifest.push"]
            max_retries = 999999
        "#;

        let result: Result<EventWebhookConfig, _> = toml::from_str(toml);
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("max_retries=999999 exceeds the supported maximum of 16"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn max_retries_defaults_to_zero() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "optional"
            events = ["blob.push"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.max_retries, 0);
    }
}
