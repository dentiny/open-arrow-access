//! Process-isolated release benchmarks; see ../README.md.
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::*;
use open_arrow_access_file_format::{
    atif::{AtifAccessor, AtifFormat},
    csv::CsvAccessor,
    json::JsonAccessor,
    parquet::ParquetAccessor,
    warc::WarcAccessor,
    xml::{XmlAccessor, XmlField, XmlSchema},
};
use open_arrow_access_storage::OpenDalAccessor;
use std::{
    hint::black_box,
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Default)]
struct Counts {
    calls: AtomicU64,
    bytes: AtomicU64,
}
#[derive(Clone)]
struct Memory {
    data: Bytes,
    chunk: usize,
    counts: Arc<Counts>,
}
struct Input {
    source: Memory,
    offset: usize,
    metadata: ByteMetadata,
}
#[async_trait]
impl Accessor for Memory {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            ..Default::default()
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(Input {
            source: self.clone(),
            offset: 0,
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.data.len() as u64),
                version: None,
            },
        }))
    }
}
#[async_trait]
impl Reader for Input {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        self.source.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.offset == self.source.data.len() {
            return Ok(None);
        }
        let end = (self.offset + self.source.chunk).min(self.source.data.len());
        let bytes = self.read_range(self.offset as u64..end as u64).await?;
        self.offset = end;
        Ok(Some(bytes))
    }
}
#[async_trait]
impl ByteReader for Input {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        validate_byte_range(&range, self.source.data.len() as u64, "benchmark")?;
        self.source.counts.calls.fetch_add(1, Ordering::Relaxed);
        self.source
            .counts
            .bytes
            .fetch_add(range.end - range.start, Ordering::Relaxed);
        Ok(self
            .source
            .data
            .slice(range.start as usize..range.end as usize))
    }
}
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}
fn generate(kind: &str, rows: usize) -> Vec<u8> {
    match kind {
        "json" => (0..rows).map(|i| format!("{{\"id\":{i},\"text\":\"corpus text with unicode 你好 and escaped \\\"quotes\\\"\"}}\n")).collect::<String>().into_bytes(),
        "csv" => (0..rows).map(|i| format!("{i},corpus text with unicode 你好 and no delimiter\n")).collect::<String>().into_bytes(),
        "xml" | "xml-infer" => format!("<rows>{}</rows>", (0..rows).map(|i| format!("<row><id>{i}</id><text>corpus text 你好 &amp; more</text></row>")).collect::<String>()).into_bytes(),
        "atif" => {
            let line = include_str!("../../testdata/atif/trajectories.jsonl").lines().find(|line| !line.trim().is_empty()).unwrap();
            format!("{line}\n").repeat(rows).into_bytes()
        }
        "warc" => include_bytes!("../../testdata/warc/good_resource.warc").repeat(rows),
        "parquet" => {
            let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from_iter_values(0..rows as i64)), Arc::new(StringArray::from_iter_values((0..rows).map(|i| format!("corpus text 你好 {i}"))))]).unwrap();
            let mut data = Vec::new();
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut data, schema(), None).unwrap();
            writer.write(&batch).unwrap(); writer.close().unwrap(); data
        }
        "storage" => (0..32*1024*1024).map(|i| (i % 251) as u8).collect(),
        _ => panic!("unknown format: {kind}"),
    }
}
async fn records<A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>>(
    a: A,
    request: ReadRequest,
    expected: usize,
) -> (u64, usize, f64) {
    let start = Instant::now();
    let mut reader = a.open(request).await.unwrap();
    let mut rows = 0;
    let mut memory = 0;
    let mut first = None;
    while let Some(batch) = reader.next().await.unwrap() {
        first.get_or_insert_with(|| start.elapsed().as_secs_f64());
        rows += batch.num_rows() as u64;
        memory = memory.max(batch.get_array_memory_size());
        black_box(&batch);
    }
    assert_eq!(rows, expected as u64);
    (rows, memory, first.unwrap_or_default())
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    let kind = args.get(1).map(String::as_str).unwrap_or("json");
    let rows = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(16384);
    let chunk = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(65536);
    let batch = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(1024);
    let mode = args.get(5).map(String::as_str).unwrap_or("all");
    let backend = args.get(6).map(String::as_str).unwrap_or("memory");
    let data = generate(kind, rows);
    let input_bytes = data.len();
    let counts = Arc::new(Counts::default());
    let source = Memory {
        data: Bytes::from(data),
        chunk,
        counts: counts.clone(),
    };
    let projection = (mode == "project").then(|| {
        vec![
            if kind == "atif" {
                "trajectory_id"
            } else {
                "id"
            }
            .into(),
        ]
    });
    let request = ReadRequest::new("data").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(batch).unwrap(),
        projection,
    });
    // Prepare filesystem data and backend outside the timed scan.
    let dir = tempfile::tempdir().unwrap();
    let storage = if kind == "storage" {
        let op = if backend == "s3" {
            opendal_http_transport_reqwest::install_default();
            opendal::Operator::new(
                opendal::services::S3::default()
                    .endpoint(&std::env::var("BENCH_HTTP_URL").expect("BENCH_HTTP_URL required"))
                    .bucket("benchmark")
                    .region("us-east-1")
                    .access_key_id("benchmark")
                    .secret_access_key("benchmark")
                    .disable_config_load(),
            )
            .unwrap()
        } else if backend == "http" {
            opendal_http_transport_reqwest::install_default();
            opendal::Operator::new(
                opendal_service_http::Http::default()
                    .endpoint(&std::env::var("BENCH_HTTP_URL").expect("BENCH_HTTP_URL required")),
            )
            .unwrap()
        } else {
            std::fs::write(dir.path().join("data"), &source.data).unwrap();
            opendal::Operator::new(
                opendal::services::Fs::default().root(dir.path().to_str().unwrap()),
            )
            .unwrap()
        };
        Some(OpenDalAccessor::new(op).with_chunk_size(NonZeroUsize::new(chunk).unwrap()))
    } else {
        None
    };
    let cpu_start = cpu_time::ProcessTime::now();
    let start = Instant::now();
    let (output_rows, max_batch_bytes, first_seconds) = match kind {
        "csv" => records(CsvAccessor::new(source, schema()), request, rows).await,
        "json" => records(JsonAccessor::new(source, schema()), request, rows).await,
        "parquet" => records(ParquetAccessor::new(source), request, rows).await,
        "atif" => {
            records(
                AtifAccessor::new(source, AtifFormat::JsonLines),
                request,
                rows,
            )
            .await
        }
        "xml" => {
            records(
                XmlAccessor::new(source, "/rows/row").with_schema(
                    XmlSchema::new(vec![
                        XmlField::new("id", "id", DataType::Int64, false),
                        XmlField::new("text", "text", DataType::Utf8, false),
                    ])
                    .unwrap(),
                ),
                request,
                rows,
            )
            .await
        }
        "xml-infer" => records(XmlAccessor::new(source, "/rows/row"), request, rows).await,
        "warc" => {
            let mut reader = WarcAccessor::new(source).open("data".into()).await.unwrap();
            let mut n = 0;
            let mut max_body = 0;
            let mut first = 0.;
            while let Some(record) = reader.next().await.unwrap() {
                if n == 0 {
                    first = start.elapsed().as_secs_f64();
                }
                max_body = max_body.max(record.body().len());
                black_box(record);
                n += 1;
            }
            assert_eq!(n, rows as u64);
            (n, max_body, first)
        }
        "storage" => {
            let mut reader = storage
                .unwrap()
                .open(ByteReadRequest::new("data").with_options(ByteReadOptions {
                    io_concurrency: NonZeroUsize::new(batch).unwrap(),
                    io_chunk_size: Some(NonZeroUsize::new(2 * 1024 * 1024).unwrap()),
                }))
                .await
                .unwrap();
            let mut total = 0;
            let mut first = 0.;
            if mode == "ranges" || mode == "empty-ranges" {
                let ranges: Vec<_> = (0..4096u64)
                    .map(|i| {
                        let p = i * 8192;
                        p..p + if mode == "empty-ranges" { 0 } else { 64 }
                    })
                    .collect();
                for bytes in reader.read_ranges(ranges).await.unwrap() {
                    total += bytes.len();
                    black_box(bytes);
                }
                first = start.elapsed().as_secs_f64();
                assert_eq!(total, if mode == "empty-ranges" { 0 } else { 4096 * 64 });
            } else {
                while let Some(bytes) = reader.next().await.unwrap() {
                    if total == 0 {
                        first = start.elapsed().as_secs_f64();
                    }
                    total += bytes.len();
                    black_box(bytes);
                }
                assert_eq!(total, input_bytes);
            }
            (total as u64, 0, first)
        }
        _ => unreachable!(),
    };
    println!(
        "{}",
        serde_json::json!({"format":kind,"backend":backend,"mode":mode,"rows":rows,"chunk":chunk,"batch":batch,"input_bytes":input_bytes,"output_units":output_rows,"elapsed_seconds":start.elapsed().as_secs_f64(),"cpu_seconds":cpu_start.elapsed().as_secs_f64(),"first_seconds":first_seconds,"max_batch_bytes":max_batch_bytes,"source_calls":counts.calls.load(Ordering::Relaxed),"source_bytes":counts.bytes.load(Ordering::Relaxed)})
    );
}
