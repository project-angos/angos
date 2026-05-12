use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

use crate::{configuration::RegexPattern, event_webhook::event::EventKind, secret::Secret};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryPolicy {
    Required,
    Optional,
    Async,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "EventWebhookConfigFields")]
pub struct EventWebhookConfig {
    pub url: Url,
    pub policy: DeliveryPolicy,
    pub token: Option<Secret<String>>,
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub events: Vec<EventKind>,
    pub repository_filter: Option<Vec<RegexPattern>>,
}

#[derive(Deserialize)]
struct EventWebhookConfigFields {
    url: Url,
    policy: DeliveryPolicy,
    #[serde(default, deserialize_with = "deserialize_token")]
    token: Option<Secret<String>>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default, deserialize_with = "deserialize_max_retries")]
    max_retries: u32,
    events: Vec<EventKind>,
    #[serde(default)]
    repository_filter: Option<Vec<RegexPattern>>,
}

impl TryFrom<EventWebhookConfigFields> for EventWebhookConfig {
    type Error = String;

    fn try_from(fields: EventWebhookConfigFields) -> Result<Self, Self::Error> {
        if fields.events.is_empty() {
            return Err("event webhook must have at least one event".to_string());
        }

        Ok(Self {
            url: fields.url,
            policy: fields.policy,
            token: fields.token,
            timeout_ms: fields.timeout_ms,
            max_retries: fields.max_retries,
            events: fields.events,
            repository_filter: fields.repository_filter,
        })
    }
}

fn default_timeout_ms() -> u64 {
    5000
}

fn deserialize_token<'de, D>(deserializer: D) -> Result<Option<Secret<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let token = Option::<Secret<String>>::deserialize(deserializer)?;
    if token
        .as_ref()
        .is_some_and(|token| token.expose().is_empty())
    {
        return Err(serde::de::Error::custom(
            "event webhook token must not be empty",
        ));
    }
    Ok(token)
}

fn deserialize_max_retries<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    const MAX_RETRIES: u32 = 16;
    let max_retries = u32::deserialize(deserializer)?;
    if max_retries > MAX_RETRIES {
        return Err(serde::de::Error::custom(format!(
            "max_retries={max_retries} exceeds the supported maximum of {MAX_RETRIES}"
        )));
    }
    Ok(max_retries)
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
    fn empty_token_fails_at_deserialize_time() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            token = ""
            events = ["manifest.push"]
        "#;

        let err = toml::from_str::<EventWebhookConfig>(toml).unwrap_err();
        assert!(
            err.to_string()
                .contains("event webhook token must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_events_fail_at_deserialize_time() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = []
        "#;

        let err = toml::from_str::<EventWebhookConfig>(toml).unwrap_err();
        assert!(
            err.to_string()
                .contains("event webhook must have at least one event"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn valid_webhook_config_deserializes() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "optional"
            events = ["manifest.push"]
            repository_filter = ["^myapp/.*"]
        "#;

        assert!(toml::from_str::<EventWebhookConfig>(toml).is_ok());
    }

    #[test]
    fn valid_webhook_config_without_filter_deserializes() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "async"
            events = ["tag.create", "tag.delete"]
        "#;

        assert!(toml::from_str::<EventWebhookConfig>(toml).is_ok());
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
