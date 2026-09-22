//! The shape every policy table shares, `access_policy`, `scan` and `index`
//! alike: `default` decides what no rule matches, and a matching rule decides
//! the opposite.

use serde::Deserialize;

use crate::policy::CelRule;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct PolicyConfig<A> {
    /// What a request or image no rule matches gets. Absent, each policy
    /// falls back to its own: deny for access, skip for scan and index.
    pub default: Option<A>,
    /// A matching rule gives the opposite of `default`.
    #[serde(default)]
    pub rules: Vec<CelRule>,
}

impl<A> PolicyConfig<A> {
    /// Whether the table decides anything at all.
    pub fn is_set(&self) -> bool {
        self.default.is_some() || !self.rules.is_empty()
    }
}
