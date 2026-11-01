use serde::Deserialize;

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
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(try_from = "f64")]
pub struct SamplingRate(f64);

impl TryFrom<f64> for SamplingRate {
    type Error = String;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            return Err(format!(
                "sampling_rate must be a finite number in [0.0, 1.0], got {value}"
            ));
        }
        Ok(Self(value))
    }
}

impl From<SamplingRate> for f64 {
    fn from(rate: SamplingRate) -> Self {
        rate.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::configuration::observability::SamplingRate;

    #[test]
    fn valid_values_are_preserved() {
        for &v in &[0.0_f64, 0.5, 1.0] {
            let rate = SamplingRate::try_from(v).expect("valid rate");
            assert_eq!(f64::from(rate).to_bits(), v.to_bits());
        }
    }

    #[test]
    fn invalid_values_are_rejected() {
        for &v in &[f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.5_f64, 1.5] {
            assert!(SamplingRate::try_from(v).is_err(), "expected error for {v}");
        }
    }

    #[test]
    fn toml_error_propagates() {
        let err = toml::from_str::<HashMap<String, SamplingRate>>("rate = 1.5")
            .unwrap_err()
            .to_string();
        assert!(err.contains("[0.0, 1.0]"), "unexpected error: {err}");
    }
}
