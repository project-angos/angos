use serde::Deserialize;

use crate::configuration::listeners::ListenerBaseConfig;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct InsecureListenerConfig {
    #[serde(flatten)]
    pub base: ListenerBaseConfig,
}
