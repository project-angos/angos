use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use url::Url;

use angos_secret::Secret;

use crate::{configuration::RegexPattern, event_webhook::event::EventKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryPolicy {
    Required,
    Optional,
    Async,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventWebhookConfig {
    pub url: Url,
    pub policy: DeliveryPolicy,
    #[serde(default, deserialize_with = "deserialize_token")]
    pub token: Option<Secret<String>>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// The explicit retry budget; [`Self::max_retries`] fills in the policy's
    /// default when absent.
    #[serde(default, deserialize_with = "deserialize_max_retries")]
    pub max_retries: Option<u32>,
    pub events: Vec<EventKind>,
    #[serde(default)]
    pub repository_filter: Option<Vec<RegexPattern>>,
}

/// Default retry budget for `required`-policy webhooks: a transient delivery
/// failure would otherwise fail the client operation outright, so a short
/// backed-off retry burst runs first. Other policies default to no retries.
const REQUIRED_POLICY_DEFAULT_MAX_RETRIES: u32 = 3;

impl EventWebhookConfig {
    #[must_use]
    pub fn max_retries(&self) -> u32 {
        self.max_retries.unwrap_or(match self.policy {
            DeliveryPolicy::Required => REQUIRED_POLICY_DEFAULT_MAX_RETRIES,
            DeliveryPolicy::Optional | DeliveryPolicy::Async => 0,
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
        return Err(D::Error::custom("event webhook token must not be empty"));
    }
    Ok(token)
}

fn deserialize_max_retries<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    const MAX_RETRIES: u32 = 16;
    let max_retries = Option::<u32>::deserialize(deserializer)?;
    if let Some(value) = max_retries
        && value > MAX_RETRIES
    {
        return Err(D::Error::custom(format!(
            "max_retries={value} exceeds the supported maximum of {MAX_RETRIES}"
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
        assert_eq!(config.max_retries(), 3);
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
        assert_eq!(config.max_retries(), 0);
        assert_eq!(config.events.len(), 1);
        assert_eq!(config.events[0], EventKind::BlobPush);
        assert_eq!(config.repository_filter, None);
    }

    #[test]
    fn deserialize_webhook_config_async_policy() {
        let toml = r#"
            url = "https://example.com/async-hook"
            policy = "async"
            events = ["manifest.push", "manifest.pull", "manifest.delete", "blob.push", "blob.pull", "tag.create", "tag.delete"]
        "#;

        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.policy, DeliveryPolicy::Async);
        assert_eq!(config.events.len(), 7);
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

    /// A `required`-policy webhook without an explicit `max_retries` gets the
    /// short default burst; an explicit zero is respected.
    #[test]
    fn required_policy_defaults_to_short_retry_burst() {
        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = ["manifest.push"]
        "#;
        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.max_retries(), 3);

        let toml = r#"
            url = "https://example.com/webhook"
            policy = "required"
            events = ["manifest.push"]
            max_retries = 0
        "#;
        let config: EventWebhookConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.max_retries(), 0, "an explicit zero must win");
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
        assert_eq!(config.max_retries(), 16);
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
}
