use std::{
    io::Write,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes,
    Capability, ReadRequest, Reader, RecordBatch, RecordReader, Result, ScanPlanner, async_trait,
    bytes_reader, into_stream,
};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::{
    csv::CsvAccessor, json::JsonAccessor, warc::WarcArrowAccessor,
};
use open_arrow_access_layers::ParallelReadLayer;
use tokio::sync::Semaphore;

#[derive(Clone)]
struct GatedStorage {
    data: Arc<Vec<Bytes>>,
    enabled: Arc<AtomicBool>,
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

struct GatedReader {
    inner: Box<dyn ByteReader>,
    state: GatedStorage,
    gated: bool,
}

impl Drop for GatedReader {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Reader for GatedReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;

    fn capability(&self) -> Capability {
        self.inner.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        self.inner.metadata()
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if !self.gated && self.state.enabled.load(Ordering::SeqCst) {
            self.gated = true;
            self.state.started.add_permits(1);
            self.state.release.acquire().await.unwrap().forget();
        }
        self.inner.next().await
    }
}
impl ByteReader for GatedReader {}

#[async_trait]
impl Accessor for GatedStorage {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let index: usize = request.object.locator().parse().unwrap();
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        Ok(Box::new(GatedReader {
            inner: Box::new(bytes_reader(self.data[index].clone(), Some(request.object))),
            state: self.clone(),
            gated: false,
        }))
    }
}

#[async_trait]
impl ByteAccessor for GatedStorage {}

async fn collect(reader: Box<dyn RecordReader>) -> Vec<RecordBatch> {
    into_stream(reader).try_collect().await.unwrap()
}

fn source(format: &str) -> GatedStorage {
    let data = (0..3).map(|id| {
        let text = match format {
            "json" => format!("{{\"id\":{id}}}\n{{\"id\":{id}}}\n"),
            "csv" => format!("id\n{id}\n{id}\n"),
            "warc" => (0..2).map(|row| format!(
                "WARC/1.1\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:test:{id}:{row}>\r\nWARC-Date: 2020-07-08T02:52:55Z\r\nContent-Length: 1\r\n\r\nx\r\n\r\n"
            )).collect::<String>(),
            _ => unreachable!(),
        };
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(text.as_bytes()).unwrap();
        Bytes::from(gzip.finish().unwrap())
    }).collect();
    GatedStorage {
        data: Arc::new(data),
        enabled: Arc::new(AtomicBool::new(false)),
        started: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
        active: Arc::new(AtomicUsize::new(0)),
        peak: Arc::new(AtomicUsize::new(0)),
    }
}

fn files(
    storage: GatedStorage,
    format: &str,
    union: bool,
) -> FileSetAccessor<Arc<dyn ScanPlanner<Request = ReadRequest>>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let input = storage.layer(DecompressionLayer::new(Gzip));
    let format: Arc<dyn ScanPlanner<Request = ReadRequest>> = match format {
        "json" => Arc::new(JsonAccessor::new(input, schema)),
        "csv" => Arc::new(CsvAccessor::new(input, schema)),
        "warc" => Arc::new(WarcArrowAccessor::new(input)),
        _ => unreachable!(),
    };
    let files = FileSetAccessor::new(format).with_file_planning();
    if union {
        files.with_union_by_name()
    } else {
        files
    }
}

#[tokio::test]
async fn compressed_file_discovery_overlaps_within_budget_and_keeps_order() {
    for format in ["json", "csv", "warc"] {
        for union in [false, true] {
            for concurrency in [1, 2, 3] {
                let storage = source(format);
                let files = files(storage.clone(), format, union);
                let mut request = FileSetRequest::new(["2", "0", "1", "2"]);
                request.options.batch_size = NonZeroUsize::MIN;
                let mut single = FileSetRequest::new(["0"]);
                single.options.batch_size = NonZeroUsize::MIN;
                let single = files.plan_scan(single).await.unwrap();
                assert_eq!(
                    single
                        .partitions
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap()
                        .len(),
                    2
                );
                let expected = collect(files.open(request.clone()).await.unwrap()).await;
                assert_eq!(storage.active.load(Ordering::SeqCst), 0);
                storage.peak.store(0, Ordering::SeqCst);
                let parallel = files.layer(ParallelReadLayer::new(
                    NonZeroUsize::new(concurrency).unwrap(),
                    NonZeroUsize::MIN,
                ));
                let reader = parallel.open(request).await.unwrap();
                storage.enabled.store(true, Ordering::SeqCst);
                let task = tokio::spawn(collect(reader));
                // Every resident file must start downloading before any file is
                // released. The old chained partition stream starts only one.
                tokio::time::timeout(
                    Duration::from_secs(5),
                    storage.started.acquire_many(concurrency as u32),
                )
                .await
                .expect("resident files must read concurrently")
                .unwrap()
                .forget();
                assert_eq!(storage.active.load(Ordering::SeqCst), concurrency);
                assert_eq!(storage.started.available_permits(), 0);
                storage.release.add_permits(4);
                let actual = tokio::time::timeout(Duration::from_secs(5), task)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    actual, expected,
                    "{format}, union={union}, concurrency={concurrency}"
                );
                assert!(storage.peak.load(Ordering::SeqCst) <= concurrency);
                assert_eq!(storage.active.load(Ordering::SeqCst), 0);
            }
        }
    }
}

#[tokio::test]
async fn dropping_blocked_file_workers_releases_all_inputs() {
    for union in [false, true] {
        let storage = source("json");
        let parallel = files(storage.clone(), "json", union).layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ));
        let reader = parallel
            .open(FileSetRequest::new(["0", "1", "2"]))
            .await
            .unwrap();
        storage.enabled.store(true, Ordering::SeqCst);
        let task = tokio::spawn(collect(reader));
        tokio::time::timeout(Duration::from_secs(5), storage.started.acquire_many(2))
            .await
            .unwrap()
            .unwrap()
            .forget();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), async {
            while storage.active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(storage.started.available_permits(), 0);
    }
}
