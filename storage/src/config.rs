use std::time::Duration;

use open_arrow_access_core::Result;
use opendal::{Operator, layers::TimeoutLayer};

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
    /// Exponential backoff cap and maximum accepted server delay.
    /// A larger Retry-After stops retries instead of retrying early.
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
    /// uses OpenDAL temporary errors; 429/503 also honor Retry-After.
    /// Server delays above max_delay stop retries instead of retrying early.
    pub fn apply(&self, mut operator: Operator) -> Result<Operator> {
        self.validate()?;
        if let Some(timeout) = &self.timeout {
            operator = operator.layer(
                TimeoutLayer::new()
                    .with_timeout(timeout.operation_timeout)
                    .with_io_timeout(timeout.io_timeout),
            );
        }
        if let Some(retry) = &self.retry {
            operator = operator.layer(crate::retry::HttpRetryLayer::new(retry));
        }
        Ok(operator)
    }
}
