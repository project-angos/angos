use std::{
    borrow::Borrow,
    fmt::{Display, Formatter},
    ops::Deref,
    str::FromStr,
    sync::LazyLock,
};

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::oci::Error;

// RFC 6838 media type: `type/subtype` where each name is a `restricted-name`
// (alphanumeric first char, then alphanumerics and `!#$&-^_.+`, max 127 chars),
// with an optional trailing parameter section so a `Content-Type` header value
// such as `application/json; charset=utf-8` is accepted. The `+suffix` of a
// structured syntax (e.g. `+json`) is already covered by the `+` in the name.
static MEDIA_TYPE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}(?:[ \t]*;.*)?$",
    )
    .unwrap()
});

/// A validated media type as carried by a manifest's `mediaType`, a descriptor,
/// and the `Content-Type` of a manifest request. The private field forces
/// construction through the validating constructors so a stored or wire value is
/// always a well-formed RFC 6838 media type.
#[derive(Clone, Debug, Ord, Eq, Hash, PartialEq, PartialOrd)]
pub struct MediaType(String);

impl MediaType {
    /// The single validation predicate shared by every constructor.
    fn is_valid(s: &str) -> bool {
        MEDIA_TYPE_REGEX.is_match(s)
    }

    pub fn new(s: &str) -> Result<Self, Error> {
        if Self::is_valid(s) {
            Ok(Self(s.to_owned()))
        } else {
            Err(Error::InvalidMediaType(s.to_string()))
        }
    }
}

impl FromStr for MediaType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for MediaType {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if Self::is_valid(&s) {
            Ok(Self(s))
        } else {
            Err(Error::InvalidMediaType(s))
        }
    }
}

impl TryFrom<&str> for MediaType {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl Display for MediaType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for MediaType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for MediaType {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for MediaType {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for MediaType {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for MediaType {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for MediaType {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<MediaType> for String {
    fn eq(&self, other: &MediaType) -> bool {
        self == &other.0
    }
}

impl Serialize for MediaType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for MediaType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_from(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_media_types() {
        for input in [
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.layer.v1.tar+gzip",
            "application/vnd.oci.image.layer.v1.tar",
            "application/json",
            "text/plain",
            "application/octet-stream",
        ] {
            assert!(MediaType::new(input).is_ok(), "'{input}' must be accepted");
        }
    }

    #[test]
    fn test_content_type_with_parameters_accepted() {
        let mt = MediaType::new("application/json; charset=utf-8").unwrap();
        assert_eq!(mt.as_ref(), "application/json; charset=utf-8");
    }

    #[test]
    fn test_invalid_media_types_rejected() {
        for input in [
            "",
            "application",
            "application/",
            "/json",
            "appl ication/json",
            "application/vnd oci",
        ] {
            assert!(MediaType::new(input).is_err(), "'{input}' must be rejected");
        }
    }

    #[test]
    fn test_from_str_and_try_from() {
        assert!(MediaType::from_str("application/json").is_ok());
        assert!(MediaType::from_str("nope").is_err());
        assert!(MediaType::try_from("application/json").is_ok());
        assert!(MediaType::try_from("application/json".to_string()).is_ok());
        assert!(MediaType::try_from("nope".to_string()).is_err());
    }

    #[test]
    fn test_display_deref_and_eq() {
        let mt = MediaType::new("application/json").unwrap();
        assert_eq!(mt.to_string(), "application/json");
        assert!(mt.starts_with("application/"));
        assert_eq!(mt, *"application/json");
        assert_eq!(mt, "application/json");
    }

    #[test]
    fn test_serialize_round_trip() {
        let mt = MediaType::new("application/vnd.oci.image.manifest.v1+json").unwrap();
        let json = serde_json::to_string(&mt).unwrap();
        assert_eq!(json, "\"application/vnd.oci.image.manifest.v1+json\"");
        let parsed: MediaType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, mt);
    }

    #[test]
    fn test_deserialize_invalid_rejected() {
        let result: Result<MediaType, _> = serde_json::from_str("\"not a media type\"");
        assert!(result.is_err());
    }
}
