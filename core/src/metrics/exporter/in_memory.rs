use std::sync::{Arc, Mutex};

use super::{MetricAggregate, MetricSummary, MetricsExporter, MetricsSnapshot};
use crate::metrics::{MetricEvent, MetricValue};

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
