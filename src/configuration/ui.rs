use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct UiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "UiConfig::default_name")]
    pub name: String,
    #[serde(default)]
    pub oidc: Option<UiOidcConfig>,
}

/// How the UI signs a browser in. The issuer is not repeated here: it is read
/// from the named `auth.oidc` provider, so the browser can only be sent to an
/// issuer the registry validates tokens from.
#[derive(Clone, Debug, Deserialize)]
pub struct UiOidcConfig {
    pub provider: String,
    pub client_id: String,
    #[serde(default = "UiOidcConfig::default_scopes")]
    pub scopes: String,
}

impl Default for UiConfig {
    fn default() -> Self {
        UiConfig {
            enabled: false,
            name: UiConfig::default_name(),
            oidc: None,
        }
    }
}

impl UiConfig {
    fn default_name() -> String {
        "Angos".to_string()
    }
}

impl UiOidcConfig {
    /// The OAuth `scope` parameter, verbatim. `openid` is what makes the
    /// authorization request an OIDC one and yields the ID token the UI sends.
    fn default_scopes() -> String {
        "openid profile email".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::UiConfig;

    #[test]
    fn oidc_defaults_to_the_openid_scopes() {
        let config: UiConfig = toml::from_str(
            r#"
            enabled = true
            [oidc]
            provider = "dex"
            client_id = "angos-ui"
            "#,
        )
        .unwrap();

        assert_eq!(config.oidc.unwrap().scopes, "openid profile email");
    }
}
