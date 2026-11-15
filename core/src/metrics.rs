//! Instance-scoped metrics without a global recorder, runtime, or background tasks.
//!
//! Exporters run synchronously on the read path: implementations must be fast,
//! nonblocking and must not panic. Clone an exporter to share one scope across
//! layers and OpenDAL operators. No metric includes object paths or URLs.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
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

/// Default exporter: shared, cumulative in-memory aggregates with no external I/O.
///
/// Snapshots are consistent under one lock and never reset live gauges. Storage
/// is bounded by the number of metric/operation pairs, not the number of reads.
/// Callers supplying their own events should likewise use a finite vocabulary.
#[derive(Clone, Debug, Default)]
pub struct InMemoryExporter {
    inner: Arc<Mutex<MetricsSnapshot>>,
}

impl InMemoryExporter {
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl MetricsExporter for InMemoryExporter {
    fn record(&self, event: MetricEvent) {
        let mut values = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let value = values
            .entry((event.name, event.operation))
            .or_insert_with(|| match event.value {
                MetricValue::Counter(_) => MetricAggregate::Counter(0),
                MetricValue::Gauge(_) => MetricAggregate::Gauge(0),
                MetricValue::Observation(_) => MetricAggregate::Summary(MetricSummary::default()),
            });
        match (value, event.value) {
            (MetricAggregate::Counter(total), MetricValue::Counter(n)) => {
                *total = total.saturating_add(n)
            }
            (MetricAggregate::Gauge(total), MetricValue::Gauge(n)) => {
                *total = total.saturating_add(n)
            }
            (MetricAggregate::Summary(summary), MetricValue::Observation(n))
                if n.is_finite() && n >= 0.0 =>
            {
                summary.count = summary.count.saturating_add(1);
                summary.sum += n;
                summary.max = summary.max.max(n);
            }
            // Metric types must remain stable; malformed custom events are ignored.
            _ => {}
        }
    }
}
