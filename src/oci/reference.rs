use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
    sync::LazyLock,
};

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::oci::{Digest, Error};

static TAG_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\w[\w.-]{0,127}$").unwrap());

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub enum Reference {
    Tag(String),
    Digest(Digest),
}

impl Reference {
    pub fn as_str(&self) -> &str {
        match self {
            Reference::Tag(s) => s,
            Reference::Digest(d) => d.as_str(),
        }
    }
}

impl FromStr for Reference {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::InvalidFormat(
                "Reference cannot be empty".to_string(),
            ));
        }

        if s.contains(':') {
            Ok(Reference::Digest(Digest::try_from(s)?))
        } else if TAG_REGEX.is_match(s) {
            Ok(Reference::Tag(s.to_string()))
        } else {
            Err(Error::InvalidFormat(format!("Invalid reference: '{s}'")))
        }
    }
}

impl TryFrom<String> for Reference {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Reference::Tag(s) => write!(f, "{s}"),
            Reference::Digest(d) => write!(f, "{d}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tag() {
        let Reference::Tag(tag) = Reference::from_str("latest").unwrap() else {
            panic!("Expected tag");
        };
        assert_eq!(tag, "latest");
    }

    #[test]
    fn test_parse_digest() {
        let input = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let Reference::Digest(digest) = Reference::from_str(input).unwrap() else {
            panic!("Expected digest");
        };
        assert_eq!(digest.algorithm(), "sha256");
    }

    #[test]
    fn test_display() {
        assert_eq!(Reference::Tag("latest".to_string()).to_string(), "latest");
    }
}
