use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

use angos_mtls_client::ClientTls;
use angos_secret::Secret;

use crate::configuration::RegexPattern;
use crate::event_webhook::event::EventKind;

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
    #[serde(flatten)]
    pub tls: ClientTls,
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
    use std::path::Path;

    use crate::event_webhook::config::EventWebhookConfig;

    fn parse(fields: &str) -> Result<EventWebhookConfig, toml::de::Error> {
        toml::from_str(&format!(
            "url = \"https://example.com/webhook\"\nevents = [\"manifest.push\"]\n{fields}"
        ))
    }

    #[test]
    fn empty_token_fails_at_deserialize_time() {
        assert!(parse("policy = \"required\"\ntoken = \"\"").is_err());
    }

    /// Only a `required`-policy webhook defaults to a short retry burst; an
    /// explicit zero is respected.
    #[test]
    fn max_retries_defaults_by_policy() {
        let retries = |fields| parse(fields).unwrap().max_retries();
        assert_eq!(retries("policy = \"optional\""), 0);
        assert_eq!(retries("policy = \"required\""), 3);
        assert_eq!(retries("policy = \"required\"\nmax_retries = 0"), 0);
    }

    #[test]
    fn max_retries_is_bounded() {
        let config = parse("policy = \"required\"\nmax_retries = 16").unwrap();
        assert_eq!(config.max_retries(), 16);
        assert!(parse("policy = \"required\"\nmax_retries = 17").is_err());
    }

    /// An endpoint behind a private CA, or one asking for a client certificate,
    /// is reachable; half an identity is refused.
    #[test]
    fn tls_settings_parse_and_half_an_identity_is_refused() {
        let base = "policy = \"required\"\nserver_ca_bundle = \"/etc/angos/ca.pem\"\n";
        let config = parse(&format!(
            "{base}client_certificate_bundle = \"/c.pem\"\nclient_private_key = \"/k.pem\""
        ))
        .unwrap();
        assert_eq!(
            config.tls.server_ca_bundle.as_deref(),
            Some(Path::new("/etc/angos/ca.pem"))
        );
        assert!(config.tls.identity.is_some());
        assert!(parse(&format!("{base}client_private_key = \"/k.pem\"")).is_err());
    }
}
