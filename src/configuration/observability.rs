use serde::{Deserialize, Deserializer, de};

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default)]
    pub tracing: Option<TracingConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TracingConfig {
    pub endpoint: String,
    pub sampling_rate: SamplingRate,
}

/// A validated sampling rate in the closed interval `[0.0, 1.0]`.
///
/// Rejects NaN, infinite values, negatives, and values greater than 1.0 at
/// deserialisation time so that invalid configuration is caught before it
/// reaches the OpenTelemetry sampler.
#[derive(Clone, Copy, Debug)]
pub struct SamplingRate(f64);

impl SamplingRate {
    /// Build a `SamplingRate` from a raw `f64`, validating that the value is
    /// a finite number in the closed interval `[0.0, 1.0]`.
    pub fn try_from_f64(value: f64) -> Result<Self, String> {
        if !(0.0..=1.0).contains(&value) {
            return Err(format!(
                "sampling_rate must be a finite number in [0.0, 1.0], got {value}"
            ));
        }
        Ok(Self(value))
    }
}

impl<'de> Deserialize<'de> for SamplingRate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from_f64(f64::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl From<SamplingRate> for f64 {
    fn from(rate: SamplingRate) -> Self {
        rate.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize)]
    struct Wrapper {
        rate: SamplingRate,
    }

    #[test]
    fn valid_values_are_preserved() {
        for &v in &[0.0_f64, 0.5, 1.0] {
            let rate = SamplingRate::try_from_f64(v).expect("valid rate");
            assert_eq!(f64::from(rate).to_bits(), v.to_bits());
        }
    }

    #[test]
    fn invalid_values_are_rejected() {
        for &v in &[f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.5_f64, 1.5] {
            assert!(
                SamplingRate::try_from_f64(v).is_err(),
                "expected error for {v}"
            );
        }
    }

    #[test]
    fn toml_valid_parses() {
        let w = toml::from_str::<Wrapper>("rate = 0.5").unwrap();
        assert_eq!(f64::from(w.rate).to_bits(), 0.5_f64.to_bits());
    }

    #[test]
    fn toml_error_propagates() {
        let err = toml::from_str::<Wrapper>("rate = 1.5")
            .unwrap_err()
            .to_string();
        assert!(err.contains("[0.0, 1.0]"), "unexpected error: {err}");
    }
}
