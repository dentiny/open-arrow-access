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
    Capability, ReadRequest, Reader, RecordBatch, RecordReader, Result, ScanPlanner, StreamReader,
    async_trait, into_stream,
};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::{
    csv::{CsvAccessor, CsvHeader},
    json::JsonAccessor,
    warc::WarcArrowAccessor,
};
use open_arrow_access_layers::ParallelReadLayer;
use tokio::sync::Semaphore;

#[derive(Clone)]
struct GatedStorage {
    data: Arc<Vec<Vec<Bytes>>>,
    first_only: bool,
    progress: Arc<Vec<AtomicUsize>>,
    finished: Arc<Vec<Semaphore>>,
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
    index: usize,
    eof_reported: bool,
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
        if !self.gated
            && self.state.enabled.load(Ordering::SeqCst)
            && (!self.state.first_only || self.index == 0)
        {
            self.gated = true;
            self.state.started.add_permits(1);
            self.state.release.acquire().await.unwrap().forget();
        }
        let bytes = self.inner.next().await?;
        if self.state.enabled.load(Ordering::SeqCst) {
            if bytes.is_some() {
                self.state.progress[self.index].fetch_add(1, Ordering::SeqCst);
            } else if !self.eof_reported {
                self.eof_reported = true;
                self.state.finished[self.index].add_permits(1);
            }
        }
        Ok(bytes)
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
            inner: Box::new(StreamReader::bytes(
                futures::stream::iter(self.data[index].clone().into_iter().map(Ok)),
                ByteMetadata {
                    source: Some(request.object),
                    ..ByteMetadata::default()
                },
            )),
            state: self.clone(),
            gated: false,
            index,
            eof_reported: false,
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
        vec![Bytes::from(gzip.finish().unwrap())]
    }).collect();
    GatedStorage {
        data: Arc::new(data),
        first_only: false,
        progress: Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect()),
        finished: Arc::new((0..3).map(|_| Semaphore::new(0)).collect()),
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
    files_with_schema(storage, format, union, schema)
}

fn files_with_schema(
    storage: GatedStorage,
    format: &str,
    union: bool,
    schema: Arc<Schema>,
) -> FileSetAccessor<Arc<dyn ScanPlanner<Request = ReadRequest>>> {
    let input = storage
        .layer(DecompressionLayer::new(Gzip).with_chunk_size(NonZeroUsize::new(64).unwrap()));
    let format: Arc<dyn ScanPlanner<Request = ReadRequest>> = match format {
        "json" => Arc::new(JsonAccessor::new(input, schema)),
        "csv" => Arc::new(CsvAccessor::new(input, schema).with_header(CsvHeader::Present)),
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
                // Union inference retains one input plan per selection; only P
                // workers may poll those streams during execution.
                assert_eq!(
                    storage.active.load(Ordering::SeqCst),
                    if union { 4 } else { concurrency }
                );
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
                assert!(storage.peak.load(Ordering::SeqCst) <= if union { 4 } else { concurrency });
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

const STREAM_ROWS: usize = 64;

fn sustained_source(format: &str) -> GatedStorage {
    let mut storage = source(format);
    storage.first_only = true;
    let padding = "payload".repeat(300);
    storage.data = Arc::new((0..3).map(|file| {
        (0..STREAM_ROWS).map(|row| {
            let id = file * STREAM_ROWS + row;
            let text = match format {
                "json" => format!("{{\"id\":{id},\"padding\":\"{padding}\"}}\n"),
                "csv" => format!("{}{id},{padding}\n", if row == 0 { "id,padding\n" } else { "" }),
                "warc" => format!("WARC/1.1\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:test:{id}>\r\nWARC-Date: 2020-07-08T02:52:55Z\r\nContent-Length: {}\r\n\r\n{padding}\r\n\r\n", padding.len()),
                _ => unreachable!(),
            };
            // Each record is an independently delivered gzip member. The small
            // decoder chunk size prevents one network read from hiding the file.
            let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            gzip.write_all(text.as_bytes()).unwrap();
            Bytes::from(gzip.finish().unwrap())
        }).collect()
    }).collect());
    storage
}

fn sustained_files(
    storage: GatedStorage,
    format: &str,
    union: bool,
) -> FileSetAccessor<Arc<dyn ScanPlanner<Request = ReadRequest>>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("padding", DataType::Utf8, true),
    ]));
    files_with_schema(storage, format, union, schema)
}

#[tokio::test]
async fn later_files_finish_multi_chunk_downloads_while_first_file_is_blocked() {
    for format in ["json", "csv", "warc"] {
        for union in [false, true] {
            let storage = sustained_source(format);
            let files = sustained_files(storage.clone(), format, union);
            let mut request = FileSetRequest::new(["0", "1", "2", "1"]);
            request.options.batch_size = NonZeroUsize::MIN;
            let expected = collect(files.open(request.clone()).await.unwrap()).await;
            assert_eq!(storage.active.load(Ordering::SeqCst), 0);
            storage.peak.store(0, Ordering::SeqCst);
            let reader = files
                .layer(ParallelReadLayer::new(
                    NonZeroUsize::new(3).unwrap(),
                    NonZeroUsize::MIN,
                ))
                .open(request)
                .await
                .unwrap();
            storage.enabled.store(true, Ordering::SeqCst);
            let task = tokio::spawn(collect(reader));
            tokio::time::timeout(Duration::from_secs(5), storage.started.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            for file in [1, 2] {
                tokio::time::timeout(Duration::from_secs(5), storage.finished[file].acquire())
                    .await
                    .expect("later file must download to EOF before the first is released")
                    .unwrap()
                    .forget();
                // The duplicate fourth selection must not start reading just
                // because an earlier worker finished and buffered its output.
                assert_eq!(storage.progress[file].load(Ordering::SeqCst), STREAM_ROWS);
            }
            assert_eq!(storage.progress[0].load(Ordering::SeqCst), 0);
            assert!(!task.is_finished());
            assert!(storage.peak.load(Ordering::SeqCst) <= if union { 4 } else { 3 });
            storage.release.add_permits(1);
            let actual = tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(actual, expected, "{format}, union={union}");
            assert_eq!(storage.active.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn cancellation_releases_files_waiting_on_ordered_memory_delivery() {
    let storage = sustained_source("json");
    let files = sustained_files(storage.clone(), "json", true);
    let mut request = FileSetRequest::new(["0", "1", "2"]);
    request.options.batch_size = NonZeroUsize::MIN;
    let reader = files
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::MIN,
        ))
        .open(request)
        .await
        .unwrap();
    storage.enabled.store(true, Ordering::SeqCst);
    let task = tokio::spawn(collect(reader));
    for file in [1, 2] {
        tokio::time::timeout(Duration::from_secs(5), storage.finished[file].acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        while storage.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn later_file_decode_errors_remain_fail_fast_behind_a_blocked_head() {
    for union in [false, true] {
        let mut storage = sustained_source("json");
        let mut data = storage.data.as_ref().clone();
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gzip.write_all(b"{invalid json}\n").unwrap();
        *data[1].last_mut().unwrap() = Bytes::from(gzip.finish().unwrap());
        storage.data = Arc::new(data);
        let mut request = FileSetRequest::new(["0", "1"]);
        request.options.batch_size = NonZeroUsize::MIN;
        let reader = sustained_files(storage.clone(), "json", union)
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ))
            .open(request)
            .await
            .unwrap();
        storage.enabled.store(true, Ordering::SeqCst);
        let task = tokio::spawn(into_stream(reader).try_collect::<Vec<_>>());
        let error = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("a later decoding error must bypass the blocked head")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), open_arrow_access_core::ErrorKind::Decode);
        assert!(error.to_string().contains("object \"1\""));
        assert_eq!(storage.progress[0].load(Ordering::SeqCst), 0);
        tokio::time::timeout(Duration::from_secs(5), async {
            while storage.active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
