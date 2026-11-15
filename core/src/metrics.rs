//! Instance-scoped metrics without a global recorder, runtime, or background tasks.
//!
//! Exporters run synchronously on the read path: implementations must be fast,
//! nonblocking and must not panic. Clone an exporter to share one scope across
//! layers and OpenDAL operators. No metric includes object paths or URLs.

pub mod exporter;
pub use exporter::{
    InMemoryExporter, MetricAggregate, MetricSummary, MetricsExporter, MetricsSnapshot,
};

mod layer;
pub use layer::{InstrumentReader, MetricsAccessor, MetricsLayer};

/// A counter increment, gauge delta, or nonnegative observation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    Observation(f64),
}

/// One observation. Names and operation labels come from a finite vocabulary.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricEvent {
    pub name: &'static str,
    pub operation: &'static str,
    pub value: MetricValue,
}
