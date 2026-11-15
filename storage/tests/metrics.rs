use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, AccessorExt,
    metrics::{InMemoryExporter, MetricAggregate, MetricsLayer},
};
use open_arrow_access_storage::{OpenDalAccessor, opendal_metrics_layer};
use open_arrow_access_test_support::{collect_bytes, fs_operator, http::HttpFixture};

fn counter(exporter: &InMemoryExporter, name: &'static str, operation: &'static str) -> u64 {
    match exporter.snapshot().get(&(name, operation)) {
        Some(MetricAggregate::Counter(n)) => *n,
        None => 0,
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn opendal_and_reader_metrics_share_an_exporter_but_keep_byte_counts_separate() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("data"), b"abcdef").unwrap();
    let metrics = MetricsLayer::default();
    let operator = fs_operator(dir.path()).layer(opendal_metrics_layer(metrics.exporter().clone()));
    let accessor = OpenDalAccessor::new(operator)
        .with_chunk_size(NonZeroUsize::new(2).unwrap())
        .layer(metrics.clone());
    let mut reader = accessor.open("data".into()).await.unwrap();
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abcdef"[..]);
    assert_eq!(reader.read_ranges(vec![0..3, 2..5]).await.unwrap().len(), 2);
    drop(reader);
    let exporter = metrics.exporter();
    assert_eq!(counter(exporter, "oaa_bytes_delivered_total", "next"), 6);
    assert_eq!(
        counter(exporter, "oaa_bytes_delivered_total", "read_ranges"),
        6
    );
    // The overlapping ranges can be coalesced into five backend bytes.
    let io_bytes = counter(exporter, "opendal_operation_bytes", "read");
    assert!((11..=12).contains(&io_bytes), "I/O bytes: {io_bytes}");
    assert!(counter(exporter, "opendal_operations_total", "stat") >= 1);
    assert_eq!(
        exporter.snapshot()[&("opendal_operation_executing", "read")],
        MetricAggregate::Gauge(0)
    );
    assert!(accessor.open("missing".into()).await.is_err());
    assert_eq!(counter(exporter, "oaa_errors_total", "open"), 1);
    assert!(counter(exporter, "opendal_operation_errors_total", "stat") >= 1);
}

#[tokio::test]
async fn early_drop_retains_output_and_observed_io() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("data"), b"abcdef").unwrap();
    let metrics = MetricsLayer::default();
    let accessor = OpenDalAccessor::new(
        fs_operator(dir.path()).layer(opendal_metrics_layer(metrics.exporter().clone())),
    )
    .with_chunk_size(NonZeroUsize::new(2).unwrap())
    .layer(metrics.clone());
    let mut reader = accessor.open("data".into()).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), "ab");
    drop(reader);
    assert_eq!(
        counter(metrics.exporter(), "oaa_bytes_delivered_total", "next"),
        2
    );
    assert!(counter(metrics.exporter(), "opendal_operation_bytes", "read") >= 2);
    assert_eq!(
        counter(
            metrics.exporter(),
            "oaa_readers_dropped_early_total",
            "reader"
        ),
        1
    );
}

#[tokio::test]
async fn http_transport_observations_are_exported_without_a_global_recorder() {
    let fixture = HttpFixture::new();
    fixture.file("/data", "abcdef");
    let metrics = MetricsLayer::default();
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(fixture.endpoint()))
            .unwrap()
            .layer(opendal_metrics_layer(metrics.exporter().clone()));
    let accessor = OpenDalAccessor::new(operator).layer(metrics.clone());
    let mut reader = accessor.open("data".into()).await.unwrap();
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abcdef"[..]);
    drop(reader);
    let exporter = metrics.exporter();
    assert_eq!(counter(exporter, "opendal_http_response_bytes", "read"), 6);
    assert!(counter(exporter, "opendal_http_requests_total", "read") >= 1);
    assert_eq!(
        exporter.snapshot()[&("opendal_http_executing", "read")],
        MetricAggregate::Gauge(0)
    );
}
