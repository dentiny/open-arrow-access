# Read metrics

`MetricsLayer` observes byte and record accessors without changing output,
metadata, capabilities, ordering, or bulk-range optimizations. It also delegates
listing, statistics and lazy scan planning when the source supports them.

Metrics are opt-in. The default exporter accumulates information in memory;
it does not log, listen on a port, start tasks, or install a global recorder.
Cloned layers/exporters share a scope. Independently constructed defaults are
isolated. Use distinct exporters to compare different boundaries or backends.

```rust,no_run
use open_arrow_access::{
    Accessor, AccessorExt, Result,
    layers::MetricsLayer,
    storage::{OpenDalAccessor, opendal, opendal_metrics_layer},
};

async fn read(operator: opendal::Operator) -> Result<()> {
    let metrics = MetricsLayer::default();
    let operator = operator.layer(opendal_metrics_layer(metrics.exporter().clone()));
    let files = OpenDalAccessor::new(operator).layer(metrics.clone());
    let mut reader = files.open("data.bin".into()).await?;
    while reader.next().await?.is_some() {}
    drop(reader);
    let snapshot = metrics.exporter().snapshot();
    println!("{snapshot:?}");
    Ok(())
}
```

Run `cargo run --example read_metrics -- testdata/bytes/sequence.bin` for a
filesystem example. Wrap a record accessor with the same layer to collect rows
and batches; no OpenDAL dependency is needed to observe a non-storage reader.

## Metric semantics

Snapshots map `(name, operation)` to counters, gauges, or observation summaries.
Missing entries mean no observation, rather than a recorded zero. Counters are
cumulative, gauges are current values, and summaries retain only count, sum and
maximum. Snapshots do not reset the recording scope. Memory depends on the fixed
metric/operation vocabulary, not the number of reads. There are no path, URL,
bucket, or root labels. User-defined exporters/events should retain bounded
cardinality too.

| OAA metric | Meaning |
| --- | --- |
| `oaa_calls_total` | Calls started, including failed or cancelled calls |
| `oaa_executing` | Currently executing calls |
| `oaa_errors_total` | Returned errors, including listing stream errors |
| `oaa_cancelled_total` | Started call futures dropped before returning |
| `oaa_call_duration_seconds` | Wall-clock call duration, including cancelled calls; excludes time between calls |
| `oaa_bytes_delivered_total` | Bytes successfully returned at this reader boundary |
| `oaa_chunks_total` | Byte chunks returned by `next` |
| `oaa_batches_total`, `oaa_rows_total` | Record batches and rows successfully returned by `next` |
| `oaa_readers_opened_total` | Successfully opened/wrapped readers |
| `oaa_readers_completed_total` | Readers that returned EOF |
| `oaa_readers_failed_total` | Readers whose `next` returned an error |
| `oaa_readers_dropped_early_total` | Readers dropped before EOF/error, including readers used only for ranges |
| `oaa_list_entries_total` | Listing entries consumed by the caller |

Operations are `open`, `next`, `read_range`, `read_ranges`, `list`, `statistics`,
`plan_scan`, `partition_open`, and `reader` for lifecycle metrics. A planned scan
wraps partition readers lazily. Repeated `next` after terminal EOF/error does
not count another terminal result or another call. Range failures do not finalize
the sequential reader. A cancelled call is separate from a returned error;
drop a stateful reader after cancelling its pending read, per the reader contract.

Byte totals are attributed to `next`, `read_range`, or `read_ranges`. Repeated
and overlapping results count each delivered byte. An unsuccessful bulk read
contributes no delivered bytes, even if it performed partial I/O internally.
The optimized inner `read_ranges` is called once without double-counting its
internal range reads.

## OpenDAL I/O

`storage::opendal_metrics_layer(exporter)` adapts OpenDAL 0.59.2's shared
instrumentation to the same exporter. It retains OpenDAL metric names and
operation labels. Byte and entry observations become cumulative counters;
duration and rate observations become count/sum/max summaries. Executing metrics
remain gauges, and errors remain counters. The adapter also counts positive
execution starts in `opendal_operations_total` and `opendal_http_requests_total`.
OpenDAL may observe several execution stages for a read, so operation starts
are not equivalent to backend requests. HTTP request counts are distinct.

`opendal_operation_bytes` with operation `read` measures OpenDAL-observed read
bytes, including data not delivered due to buffering or prefetch. It is not a
socket/TLS traffic measurement. HTTP body observations are available separately
as `opendal_http_request_bytes` and `opendal_http_response_bytes`. Some byte and
duration observations finalize when OpenDAL streams/bodies drop: drop readers
before taking a final snapshot. OpenDAL's cancellation/early-drop error semantics
are preserved, so they may differ from OAA's separate cancellation counters.

Install the OpenDAL metrics layer before passing the operator to
`OpenDalAccessor::try_new` to place it inside the retry policy and observe
individual attempts. An outer placement measures the retry-wrapped operation.
The existing `ScanStatistics::source_bytes` describes input size, not runtime I/O;
it is independent of all these counters.

Use separate exporters before and after decompression to compare encoded and
decoded byte output. Do not sum those values, OpenDAL read bytes and Arrow rows
as if they represented the same quantity.

## Custom exporters

Exporter implementations live in the dedicated `metrics::exporter` module.
Implement `metrics::exporter::MetricsExporter` and pass it to `MetricsLayer::new(exporter)`
and `opendal_metrics_layer(exporter.clone())`. Its `record(MetricEvent)` receives
counter increments, gauge deltas and observations. Clones must share the intended
recording scope. Exporters execute synchronously on the read path and must be
fast, nonblocking and non-panicking; network exporters should aggregate locally
and arrange delivery outside read calls. The library does not own exporter tasks.

The existing `metrics::{InMemoryExporter, MetricsExporter, MetricsSnapshot, ...}`
re-exports remain available for convenience.
