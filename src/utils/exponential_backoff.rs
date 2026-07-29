use std::time::Duration;
use futures::future::err;
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct ExponentialBackoffConfig {
    #[serde(with = "humantime_serde")]
    min: Duration,
    #[serde(with = "humantime_serde")]
    max: Duration,
    mult: f32,
    jitter: f32,
}

impl Default for ExponentialBackoffConfig {
    fn default() -> Self {
        ExponentialBackoffConfig {
            min: Duration::from_secs(3),
            max: Duration::from_secs(30),
            mult: 2.0,
            jitter: 0.2,
        }
    }
}

impl ExponentialBackoffConfig {
    fn verify(&self) -> Result<(), &'static str> {
        if self.min.is_zero() {
            return Err("minimum duration cannot be zero");
        }
        if self.max < self.min {
            return Err("maximum duration can't be less than minimum duration");
        }
        if !self.mult.is_finite() {
            return Err("multiplier has to be finite");
        }
        if self.mult < 1.0 {
            return Err("multiplier has to be at least 1.0");
        }
        if !self.jitter.is_finite() {
            return Err("jitter has to be finite");
        }
        if self.jitter < 0.0 {
            return Err("jitter can't be negative");
        }
        if self.jitter > 0.5 {
            return Err("jitter can't be more than 0.5");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExponentialBackoff {
    config: ExponentialBackoffConfig,
    previous: Duration,
}

impl ExponentialBackoff {
    pub fn new(config: ExponentialBackoffConfig) -> Result<Self, &'static str> {
        config.verify()?;
        Ok(ExponentialBackoff {
            config,
            previous: Duration::ZERO,
        })
    }
    
    pub fn new_or_default(config: ExponentialBackoffConfig) -> Self {
        Self::new(config).unwrap_or_else(|e| {
            error!("Exponential backoff config is invalid: {e}");
            ExponentialBackoff::default()
        })
    }

    pub fn reset(&mut self) {
        self.previous = Duration::ZERO;
    }

    pub fn next(&mut self) -> Duration {
        let new_duration = self.previous.mul_f32(self.config.mult).clamp(self.config.min, self.config.max);

        self.previous = new_duration;

        let jitter = rand::random_range(1.0 - self.config.jitter as f64..1.0 + self.config.jitter as f64);
        new_duration.mul_f64(jitter)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::utils::exponential_backoff::ExponentialBackoffConfig;

    #[test]
    fn deserialize_default() {
        let config: ExponentialBackoffConfig = toml::from_str("").unwrap();

        assert_eq!(config, ExponentialBackoffConfig::default());
    }

    #[test]
    fn deserialize() {
        let config: ExponentialBackoffConfig = toml::from_str(r#"
            min = "1s"
            max = "10s"
            mult = 2.0
            jitter = 0.2
        "#).unwrap();

        assert_eq!(config, ExponentialBackoffConfig {
            min: Duration::from_secs(1),
            max: Duration::from_secs(10),
            mult: 2.0,
            jitter: 0.2,
        });
    }
}
