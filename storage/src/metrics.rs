use std::{fmt, sync::Arc};

use open_arrow_access_core::metrics::{MetricEvent, MetricValue, MetricsExporter};
use opendal_layer_observe_metrics_common::{
    MetricLabels, MetricValue as OpenDalValue, MetricsIntercept,
};

/// Attach OpenDAL operation and HTTP instrumentation to an exporter shared with
/// an OAA `MetricsLayer`. Does not install a process-global recorder.
///
/// Layer order determines retry accounting. Install this on the operator before
/// passing it to `OpenDalAccessor::try_new` to observe individual retry attempts.
/// Operation bytes measure data observed by OpenDAL, not socket/TLS traffic.
/// Namespace, root, paths and service-specific labels are deliberately omitted;
/// use a separate exporter per backend when that distinction is needed.
/// OpenDAL emits some byte/duration observations when its readers/bodies drop,
/// so take the final snapshot after dropping readers.
pub fn opendal_metrics_layer<E: MetricsExporter>(exporter: E) -> impl opendal::raw::Layer {
    opendal_layer_observe_metrics_common::MetricsLayer::new(Interceptor(Arc::new(exporter)))
}

#[derive(Clone)]
struct Interceptor<E>(Arc<E>);

impl<E> fmt::Debug for Interceptor<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsInterceptor").finish_non_exhaustive()
    }
}

impl<E: MetricsExporter> MetricsIntercept for Interceptor<E> {
    fn observe(&self, labels: MetricLabels, value: OpenDalValue) {
        if let OpenDalValue::OperationExecuting(n) | OpenDalValue::HttpExecuting(n) = value
            && n > 0
        {
            self.0.record(MetricEvent {
                name: if matches!(value, OpenDalValue::OperationExecuting(_)) {
                    "opendal_operations_total"
                } else {
                    "opendal_http_requests_total"
                },
                operation: labels.operation,
                value: MetricValue::Counter(n as u64),
            });
        }
        let name = value.name();
        let value = match value {
            OpenDalValue::OperationBytes(n)
            | OpenDalValue::OperationEntries(n)
            | OpenDalValue::HttpRequestBytes(n)
            | OpenDalValue::HttpResponseBytes(n) => MetricValue::Counter(n),
            OpenDalValue::OperationErrorsTotal
            | OpenDalValue::HttpConnectionErrorsTotal
            | OpenDalValue::HttpStatusErrorsTotal => MetricValue::Counter(1),
            OpenDalValue::OperationExecuting(n) | OpenDalValue::HttpExecuting(n) => {
                MetricValue::Gauge(n as i64)
            }
            OpenDalValue::OperationDurationSeconds(d)
            | OpenDalValue::OperationTtfbSeconds(d)
            | OpenDalValue::HttpRequestDurationSeconds(d)
            | OpenDalValue::HttpResponseDurationSeconds(d) => {
                MetricValue::Observation(d.as_secs_f64())
            }
            OpenDalValue::OperationBytesRate(n)
            | OpenDalValue::OperationEntriesRate(n)
            | OpenDalValue::HttpRequestBytesRate(n)
            | OpenDalValue::HttpResponseBytesRate(n) => MetricValue::Observation(n),
            _ => return,
        };
        self.0.record(MetricEvent {
            name,
            operation: labels.operation,
            value,
        });
    }
}
