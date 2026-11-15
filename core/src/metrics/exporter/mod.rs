//! Export destinations and cumulative snapshot types.

use std::collections::BTreeMap;

use super::MetricEvent;

mod in_memory;
pub use in_memory::InMemoryExporter;

/// Destination for observations. Clones must retain the same recording scope.
pub trait MetricsExporter: Clone + Send + Sync + 'static {
    fn record(&self, event: MetricEvent);
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
