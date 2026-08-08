use base64::{Engine, prelude::BASE64_STANDARD};
use serde::{Deserialize, de};
use zeroize::Zeroize;

/// Bytes written in configuration as base64, decoded when the file is parsed.
///
/// Key material belongs here rather than in a plain string: the strength is then
/// the randomness of the decoded bytes instead of whatever entropy a passphrase
/// happens to carry. Wrap it in [`Secret`](crate::secret::Secret) to keep it out
/// of logs.
#[derive(Clone, Debug, Zeroize)]
pub struct Base64String(Vec<u8>);

impl Base64String {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Base64String {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl<'de> Deserialize<'de> for Base64String {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        BASE64_STANDARD
            .decode(&encoded)
            .map(Self)
            .map_err(|e| de::Error::custom(format!("invalid base64: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(encoded: &str) -> Result<Base64String, toml::de::Error> {
        #[derive(Deserialize)]
        struct Holder {
            value: Base64String,
        }
        toml::from_str::<Holder>(&format!("value = \"{encoded}\"")).map(|holder| holder.value)
    }

    #[test]
    fn decodes_base64_with_and_without_padding() {
        assert_eq!(parse("aGk=").unwrap().as_bytes(), b"hi");
        assert_eq!(parse("aGl0").unwrap().as_bytes(), b"hit");
        assert_eq!(parse("").unwrap().as_bytes(), b"");
    }

    #[test]
    fn rejects_text_that_is_not_base64() {
        assert!(parse("not base64!").is_err());
        assert!(parse("aGk").is_err());
    }
}
