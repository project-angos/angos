use std::fmt;

use serde::Deserialize;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A sensitive value, zeroed from memory on drop and redacted from `Debug` output.
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
        assert_eq!(format!("{secret:?}"), "[REDACTED]");
    }
}
