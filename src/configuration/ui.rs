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
    fn default_disables_ui_and_uses_product_name() {
        let config = UiConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.name, "Angos");
        assert!(config.oidc.is_none());
    }

    #[test]
    fn enabled_can_be_configured() {
        let config = toml::from_str::<UiConfig>("enabled = true").unwrap();

        assert!(config.enabled);
        assert_eq!(config.name, "Angos");
    }

    #[test]
    fn name_can_be_configured() {
        let config = toml::from_str::<UiConfig>(
            r#"
            enabled = true
            name = "my-registry"
            "#,
        )
        .unwrap();

        assert!(config.enabled);
        assert_eq!(config.name, "my-registry");
    }

    #[test]
    fn oidc_defaults_to_the_openid_scopes() {
        let config = toml::from_str::<UiConfig>(
            r#"
            enabled = true
            [oidc]
            provider = "dex"
            client_id = "angos-ui"
            "#,
        )
        .unwrap();

        let oidc = config.oidc.unwrap();
        assert_eq!(oidc.provider, "dex");
        assert_eq!(oidc.client_id, "angos-ui");
        assert_eq!(oidc.scopes, "openid profile email");
    }

    #[test]
    fn oidc_scopes_can_be_configured() {
        let config = toml::from_str::<UiConfig>(
            r#"
            enabled = true
            [oidc]
            provider = "dex"
            client_id = "angos-ui"
            scopes = "openid groups"
            "#,
        )
        .unwrap();

        assert_eq!(config.oidc.unwrap().scopes, "openid groups");
    }
}
