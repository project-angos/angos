use serde::Deserialize;

use super::ListenerBaseConfig;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct InsecureListenerConfig {
    #[serde(flatten)]
    pub base: ListenerBaseConfig,
}
