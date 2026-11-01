use std::fmt;

use serde::Deserialize;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A wrapper type for sensitive values that are automatically zeroed from memory when dropped.
///
/// This type provides:
/// - Automatic memory clearing on drop via `ZeroizeOnDrop`
/// - Debug output that hides the actual value
/// - Transparent deserialization from TOML/JSON
#[derive(Clone, Default, Deserialize, PartialEq, Zeroize, ZeroizeOnDrop)]
#[serde(transparent)]
pub struct Secret<T: Zeroize>(T);

impl<T: Zeroize> Secret<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &T {
        &self.0
    }
}

impl<T: Zeroize> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_debug_redacts_value() {
        let secret = Secret::new("my-password".to_string());
        let debug_output = format!("{secret:?}");
        assert_eq!(debug_output, "[REDACTED]");
        assert!(!debug_output.contains("my-password"));
    }
}
