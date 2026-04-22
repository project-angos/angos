use serde::Deserialize;

use crate::command::server::listeners::{insecure, tls};

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ServerConfig {
    Tls(tls::Config),
    Insecure(insecure::Config),
}
