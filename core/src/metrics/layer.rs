use std::{future::Future, ops::Range, time::Instant};

use futures::{StreamExt, TryStreamExt};

use super::{InMemoryExporter, MetricsExporter};
use crate::{
    Accessor, ByteAccessor, ByteReader, Bytes, Layer, ListRequest, ObjectStream, Reader,
    RecordBatch, RecordReader, Result, ScanPlan, ScanPlanner, ScanStatistics, StatisticsProvider,
    async_trait,
};

/// Observe accessors and byte/record readers without changing their representation.
///
/// `oaa_bytes_delivered_total` counts successful byte results at this boundary,
/// including repeated/overlapping range reads. It is distinct from OpenDAL I/O
/// bytes and from decoded Arrow memory. Place separate layers before and after
/// decompression with separate exporters to compare the representations.
/// EOF/error finalizes a reader once. Dropping it before either is an early drop,
/// including a reader used exclusively for range reads. Operation durations are
/// wall-clock time spent inside calls; they exclude time between calls.
#[derive(Clone, Debug)]
pub struct MetricsLayer<E = InMemoryExporter> {
    exporter: E,
}

impl Default for MetricsLayer {
    fn default() -> Self {
        Self::new(InMemoryExporter::default())
    }
}

impl<E: MetricsExporter> MetricsLayer<E> {
    pub fn new(exporter: E) -> Self {
        Self { exporter }
    }
    pub fn exporter(&self) -> &E {
        &self.exporter
    }
}

#[derive(Clone, Debug)]
pub struct MetricsAccessor<A, E = InMemoryExporter> {
    inner: A,
    exporter: E,
}

/// Supported reader representations for [`MetricsLayer`].
/// Implemented for the shared `dyn ByteReader` and `dyn RecordReader` interfaces.
pub trait InstrumentReader: Reader {
    fn instrument<E: MetricsExporter>(self: Box<Self>, exporter: E) -> Box<Self>;
}

impl<A, E> Layer<A> for MetricsLayer<E>
where
    A: Accessor,
    A::Reader: InstrumentReader,
    E: MetricsExporter,
{
    type Layered = MetricsAccessor<A, E>;
    fn layer(&self, inner: A) -> Self::Layered {
        MetricsAccessor {
            inner,
            exporter: self.exporter.clone(),
        }
    }
}

#[async_trait]
impl<A, E> Accessor for MetricsAccessor<A, E>
where
    A: Accessor,
    A::Reader: InstrumentReader,
    E: MetricsExporter,
{
    type Request = A::Request;
    type Reader = A::Reader;
    fn capability(&self) -> <A::Reader as Reader>::Capability {
        self.inner.capability()
    }
    async fn metadata(&self, request: Self::Request) -> Result<<Self::Reader as Reader>::Metadata>
    where
        <Self::Reader as Reader>::Metadata: Clone,
    {
        self.inner.metadata(request).await
    }

    async fn inspect_metadata(
        &self,
        request: Self::Request,
    ) -> Result<Option<<Self::Reader as Reader>::Metadata>>
    where
        <Self::Reader as Reader>::Metadata: Clone,
    {
        self.inner.inspect_metadata(request).await
    }

    async fn open(&self, request: A::Request) -> Result<Box<A::Reader>> {
        Call::observe(&self.exporter, "open", self.inner.open(request))
            .await
            .map(|reader| reader.instrument(self.exporter.clone()))
    }

    async fn open_with_metadata(
        &self,
        request: A::Request,
        metadata: &<Self::Reader as Reader>::Metadata,
    ) -> Result<Box<A::Reader>> {
        Call::observe(
            &self.exporter,
            "open",
            self.inner.open_with_metadata(request, metadata),
        )
        .await
        .map(|reader| reader.instrument(self.exporter.clone()))
    }

    async fn open_sequential(&self, request: A::Request) -> Result<Box<A::Reader>> {
        Call::observe(&self.exporter, "open", self.inner.open_sequential(request))
            .await
            .map(|reader| reader.instrument(self.exporter.clone()))
    }
}

#[async_trait]
impl<A: ByteAccessor, E: MetricsExporter> ByteAccessor for MetricsAccessor<A, E> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        let entries = Call::observe(&self.exporter, "list", self.inner.list(request)).await?;
        let exporter = self.exporter.clone();
        Ok(Box::pin(entries.inspect(move |item| {
            exporter.record_counter(
                if item.is_ok() {
                    "oaa_list_entries_total"
                } else {
                    "oaa_errors_total"
                },
                "list",
                1,
            );
        })))
    }
}

#[async_trait]
impl<A: StatisticsProvider, E: MetricsExporter> StatisticsProvider for MetricsAccessor<A, E> {
    async fn statistics(&self, request: A::Request) -> Result<ScanStatistics> {
        Call::observe(&self.exporter, "statistics", self.inner.statistics(request)).await
    }
}

#[async_trait]
impl<A: ScanPlanner, E: MetricsExporter> ScanPlanner for MetricsAccessor<A, E> {
    async fn plan_scan(&self, request: A::Request) -> Result<ScanPlan> {
        let mut plan =
            Call::observe(&self.exporter, "plan_scan", self.inner.plan_scan(request)).await?;
        let exporter = self.exporter.clone();
        plan.partitions = plan
            .partitions
            .map_ok(move |partition| {
                let exporter = exporter.clone();
                Box::pin(async move {
                    Call::observe(&exporter, "partition_open", partition)
                        .await
                        .map(|reader| reader.instrument(exporter.clone()))
                }) as crate::ScanPartition
            })
            .boxed();
        Ok(plan)
    }
}

struct Call<'a, E: MetricsExporter> {
    exporter: &'a E,
    operation: &'static str,
    start: Instant,
    finished: bool,
}

impl<'a, E: MetricsExporter> Call<'a, E> {
    async fn observe<T>(
        exporter: &'a E,
        operation: &'static str,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        exporter.record_counter("oaa_calls_total", operation, 1);
        exporter.record_gauge("oaa_executing", operation, 1);
        let mut call = Self {
            exporter,
            operation,
            start: Instant::now(),
            finished: false,
        };
        let result = future.await;
        call.finished = true;
        if result.is_err() {
            exporter.record_counter("oaa_errors_total", operation, 1);
        }
        result
    }
}

impl<E: MetricsExporter> Drop for Call<'_, E> {
    fn drop(&mut self) {
        if !self.finished {
            self.exporter
                .record_counter("oaa_cancelled_total", self.operation, 1);
        }
        self.exporter.record_observation(
            "oaa_call_duration_seconds",
            self.operation,
            self.start.elapsed().as_secs_f64(),
        );
        self.exporter
            .record_gauge("oaa_executing", self.operation, -1);
    }
}

struct Measured<R: Reader + ?Sized, E: MetricsExporter> {
    inner: Box<R>,
    exporter: E,
    terminal: bool,
}

impl<R: Reader + ?Sized, E: MetricsExporter> Measured<R, E> {
    fn new(inner: Box<R>, exporter: E) -> Self {
        exporter.record_counter("oaa_readers_opened_total", "reader", 1);
        Self {
            inner,
            exporter,
            terminal: false,
        }
    }
}

impl<R: Reader + ?Sized, E: MetricsExporter> Drop for Measured<R, E> {
    fn drop(&mut self) {
        if !self.terminal {
            self.exporter
                .record_counter("oaa_readers_dropped_early_total", "reader", 1);
        }
    }
}

trait OutputMetrics {
    fn record<E: MetricsExporter>(&self, exporter: &E);
}
impl OutputMetrics for Bytes {
    fn record<E: MetricsExporter>(&self, exporter: &E) {
        exporter.record_counter("oaa_bytes_delivered_total", "next", self.len() as u64);
        exporter.record_counter("oaa_chunks_total", "next", 1);
    }
}
impl OutputMetrics for RecordBatch {
    fn record<E: MetricsExporter>(&self, exporter: &E) {
        exporter.record_counter("oaa_batches_total", "next", 1);
        exporter.record_counter("oaa_rows_total", "next", self.num_rows() as u64);
    }
}

#[async_trait]
impl<R, E> Reader for Measured<R, E>
where
    R: Reader + ?Sized,
    R::Item: OutputMetrics,
    E: MetricsExporter,
{
    type Item = R::Item;
    type Metadata = R::Metadata;
    type Capability = R::Capability;
    fn capability(&self) -> R::Capability {
        self.inner.capability()
    }
    fn metadata(&self) -> &R::Metadata {
        self.inner.metadata()
    }
    async fn next(&mut self) -> Result<Option<R::Item>> {
        if self.terminal {
            return self.inner.next().await;
        }
        let result = Call::observe(&self.exporter, "next", self.inner.next()).await;
        match &result {
            Ok(Some(item)) => item.record(&self.exporter),
            Ok(None) => {
                self.terminal = true;
                self.exporter
                    .record_counter("oaa_readers_completed_total", "reader", 1);
            }
            Err(_) => {
                self.terminal = true;
                self.exporter
                    .record_counter("oaa_readers_failed_total", "reader", 1);
            }
        }
        result
    }
}

#[async_trait]
impl<E: MetricsExporter> ByteReader for Measured<dyn ByteReader, E> {
    fn position(&self) -> Option<crate::BytePosition> {
        self.inner.position()
    }
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        let result =
            Call::observe(&self.exporter, "read_range", self.inner.read_range(range)).await;
        if let Ok(bytes) = &result {
            self.exporter.record_counter(
                "oaa_bytes_delivered_total",
                "read_range",
                bytes.len() as u64,
            );
        }
        result
    }
    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let result = Call::observe(
            &self.exporter,
            "read_ranges",
            self.inner.read_ranges(ranges),
        )
        .await;
        if let Ok(bytes) = &result {
            self.exporter.record_counter(
                "oaa_bytes_delivered_total",
                "read_ranges",
                bytes.iter().map(|b| b.len() as u64).sum(),
            );
        }
        result
    }
}

impl InstrumentReader for dyn ByteReader {
    fn instrument<E: MetricsExporter>(self: Box<Self>, exporter: E) -> Box<Self> {
        Box::new(Measured::new(self, exporter))
    }
}
impl InstrumentReader for dyn RecordReader {
    fn instrument<E: MetricsExporter>(self: Box<Self>, exporter: E) -> Box<Self> {
        Box::new(Measured::new(self, exporter))
    }
}
