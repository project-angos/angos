use std::fmt;

use regex::{Error as RegexError, Regex};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
    /// Returns [`RegexError`] when `source` is not a valid regular expression.
    pub fn compile(source: &str) -> Result<Self, RegexError> {
        let regex = Regex::new(source)?;
        Ok(Self {
            source: source.to_owned(),
            regex,
        })
    }

    /// The original pattern string as written in the configuration file.
    pub fn as_source(&self) -> &str {
        &self.source
    }

    /// Returns `true` if `text` is matched by this pattern.
    pub fn is_match(&self, text: &str) -> bool {
        self.regex.is_match(text)
    }

    /// Consumes the wrapper, returning the compiled [`Regex`].
    #[must_use]
    pub fn into_regex(self) -> Regex {
        self.regex
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
        D: Deserializer<'de>,
    {
        let source = String::deserialize(deserializer)?;
        Self::compile(&source).map_err(DeError::custom)
    }
}

impl Serialize for RegexPattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_source())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

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
        let result: Result<HashMap<String, Vec<RegexPattern>>, _> =
            toml::from_str(r#"patterns = ["[invalid"]"#);
        assert!(
            result.is_err(),
            "invalid regex must fail at deserialize time"
        );
    }
}
