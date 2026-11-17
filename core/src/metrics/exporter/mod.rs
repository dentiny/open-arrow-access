//! Export destinations and cumulative snapshot types.

use std::collections::BTreeMap;

use super::{MetricEvent, MetricValue};

mod in_memory;
pub use in_memory::InMemoryExporter;

/// Destination for observations. Clones must retain the same recording scope.
pub trait MetricsExporter: Clone + Send + Sync + 'static {
    fn record(&self, event: MetricEvent);

    /// Increment a counter in this exporter's recording scope.
    fn record_counter(&self, name: &'static str, operation: &'static str, n: u64) {
        self.record(MetricEvent {
            name,
            operation,
            value: MetricValue::Counter(n),
        });
    }

    /// Add a signed delta to a gauge in this exporter's recording scope.
    fn record_gauge(&self, name: &'static str, operation: &'static str, n: i64) {
        self.record(MetricEvent {
            name,
            operation,
            value: MetricValue::Gauge(n),
        });
    }

    /// Record one nonnegative observation in this exporter's recording scope.
    fn record_observation(&self, name: &'static str, operation: &'static str, n: f64) {
        self.record(MetricEvent {
            name,
            operation,
            value: MetricValue::Observation(n),
        });
    }
}

/// Constant-space summary; individual samples are never retained.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct MetricSummary {
    pub count: u64,
    pub sum: f64,
    pub max: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MetricAggregate {
    Counter(u64),
    Gauge(i64),
    Summary(MetricSummary),
}

/// Cumulative values keyed by `(metric name, operation)`. Missing means unobserved.
pub type MetricsSnapshot = BTreeMap<(&'static str, &'static str), MetricAggregate>;
