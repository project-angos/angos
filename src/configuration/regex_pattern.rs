use std::fmt;

use regex::Regex;
use serde::{Deserialize, Serialize, de};

/// A pre-compiled regular expression that is validated at deserialisation time.
///
/// Storing the original source alongside the compiled [`Regex`] lets the value
/// round-trip through `Serialize` as a plain string, preserving the JSON API
/// contract for external callers.
///
/// Invalid patterns are rejected when the TOML configuration is parsed, so the
/// registry fails fast with a clear error rather than silently dropping rules
/// at runtime.
#[derive(Clone)]
pub struct RegexPattern {
    source: String,
    regex: Regex,
}

impl RegexPattern {
    /// Compile a regular expression from `source`.
    ///
    /// # Errors
    ///
    /// Returns [`regex::Error`] when `source` is not a valid regular expression.
    pub fn compile(source: impl Into<String>) -> Result<Self, regex::Error> {
        let source = source.into();
        let regex = Regex::new(&source)?;
        Ok(Self { source, regex })
    }

    /// The original pattern string as written in the configuration file.
    pub fn as_source(&self) -> &str {
        &self.source
    }

    /// Returns `true` if `text` is matched by this pattern.
    pub fn is_match(&self, text: &str) -> bool {
        self.regex.is_match(text)
    }
}

impl fmt::Debug for RegexPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RegexPattern").field(&self.source).finish()
    }
}

impl PartialEq for RegexPattern {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for RegexPattern {}

impl<'de> Deserialize<'de> for RegexPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let source = String::deserialize(deserializer)?;
        Self::compile(source).map_err(de::Error::custom)
    }
}

impl Serialize for RegexPattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_source())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config(global_section: &str) -> String {
        format!(
            r#"
[server]
address = "127.0.0.1:5000"

[global]
{global_section}
"#
        )
    }

    #[test]
    fn valid_regex_compiles() {
        assert!(RegexPattern::compile("^latest$").is_ok());
        assert!(RegexPattern::compile("^dev-.*").is_ok());
        assert!(RegexPattern::compile("").is_ok());
    }

    #[test]
    fn invalid_regex_fails_compile() {
        assert!(RegexPattern::compile("[invalid").is_err());
        assert!(RegexPattern::compile("(unclosed").is_err());
    }

    #[test]
    fn is_match_delegates_to_regex() {
        let pattern = RegexPattern::compile("^latest$").unwrap();
        assert!(pattern.is_match("latest"));
        assert!(!pattern.is_match("latest-tag"));
    }

    #[test]
    fn as_source_returns_original_string() {
        let pattern = RegexPattern::compile("^dev-.*").unwrap();
        assert_eq!(pattern.as_source(), "^dev-.*");
    }

    #[test]
    fn equality_is_by_source() {
        let a = RegexPattern::compile("^latest$").unwrap();
        let b = RegexPattern::compile("^latest$").unwrap();
        let c = RegexPattern::compile("^other$").unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn serialize_emits_source_string() {
        let pattern = RegexPattern::compile("^latest$").unwrap();
        let json = serde_json::to_string(&pattern).unwrap();
        assert_eq!(json, r#""^latest$""#);
    }

    #[test]
    fn invalid_regex_fails_at_deserialize() {
        use crate::configuration::Configuration;
        let toml = minimal_config(r#"immutable_tags_exclusions = ["[invalid"]"#);
        let result = Configuration::load_from_str(&toml);
        assert!(
            result.is_err(),
            "invalid regex must fail at deserialize time"
        );
    }

    #[test]
    fn valid_regex_deserializes() {
        use crate::configuration::Configuration;
        let toml = minimal_config(r#"immutable_tags_exclusions = ["^latest$", "^dev-.*"]"#);
        let config = Configuration::load_from_str(&toml).expect("valid regex must deserialize");
        assert_eq!(config.global.immutable_tags_exclusions.len(), 2);
        assert_eq!(
            config.global.immutable_tags_exclusions[0].as_source(),
            "^latest$"
        );
        assert_eq!(
            config.global.immutable_tags_exclusions[1].as_source(),
            "^dev-.*"
        );
    }

    #[test]
    fn clone_preserves_source_and_behavior() {
        let original = RegexPattern::compile("^test-.*").unwrap();
        let cloned = original.clone();
        assert_eq!(original.as_source(), cloned.as_source());
        assert!(cloned.is_match("test-123"));
    }

    #[test]
    fn vec_serializes_as_array_of_strings() {
        let patterns = vec![
            RegexPattern::compile("^latest$").unwrap(),
            RegexPattern::compile("^dev-.*").unwrap(),
        ];
        let json = serde_json::to_string(&patterns).unwrap();
        assert_eq!(json, r#"["^latest$","^dev-.*"]"#);
    }
}
