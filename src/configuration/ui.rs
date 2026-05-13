use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct UiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "UiConfig::default_name")]
    pub name: String,
}

impl Default for UiConfig {
    fn default() -> Self {
        UiConfig {
            enabled: false,
            name: UiConfig::default_name(),
        }
    }
}

impl UiConfig {
    fn default_name() -> String {
        "Angos".to_string()
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
}
