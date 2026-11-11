use std::time::Duration;

use open_arrow_access_core::Result;
use opendal::{
    Operator,
    layers::{RetryLayer, TimeoutLayer},
};

/// Optional OpenDAL policies. Defaults leave the supplied operator unchanged.
#[derive(Clone, Debug, Default)]
pub struct StorageConfig {
    pub retry: Option<RetryConfig>,
    pub timeout: Option<TimeoutConfig>,
}

#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Retries after the initial attempt; zero disables retries.
    pub max_retries: usize,
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub factor: f32,
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            factor: 2.0,
            jitter: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimeoutConfig {
    /// Per-attempt control-operation timeout (for example, stat).
    pub operation_timeout: Duration,
    /// Per-attempt I/O-operation and body-read timeout.
    pub io_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(60),
            io_timeout: Duration::from_secs(10),
        }
    }
}

impl StorageConfig {
    /// Validate and apply policies without performing I/O. Timeout wraps the
    /// backend inside retry, so each attempt gets its own timeout. Retryability
    /// and backoff behavior are delegated to OpenDAL.
    pub fn apply(&self, mut operator: Operator) -> Result<Operator> {
        crate::validation::validate_storage_config(self)?;
        if let Some(timeout) = &self.timeout {
            operator = operator.layer(
                TimeoutLayer::new()
                    .with_timeout(timeout.operation_timeout)
                    .with_io_timeout(timeout.io_timeout),
            );
        }
        if let Some(retry) = &self.retry {
            let mut layer = RetryLayer::new()
                .with_max_times(retry.max_retries)
                .with_min_delay(retry.min_delay)
                .with_max_delay(retry.max_delay)
                .with_factor(retry.factor);
            if retry.jitter {
                layer = layer.with_jitter();
            }
            operator = operator.layer(layer);
        }
        Ok(operator)
    }
}
