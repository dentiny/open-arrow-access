//! Process-isolated, release-mode read benchmarks. See ../README.md.
mod providers;

use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteMetadata, ByteReadOptions, ByteReadRequest,
    ByteReader, Bytes, Capability, ReadOptions, ReadRequest, Reader, ReaderFactory, RecordReader,
    Result, ScanPlanner, async_trait, validate_byte_range,
};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_file_format::{
    FileAccessor,
    atif::AtifFormat,
    csv::CsvFileFormat,
    json::JsonFormat,
    parquet::ParquetFormat,
    warc::WarcArrowFormat,
    xml::{XmlField, XmlFormat, XmlSchema},
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_storage::OpenDalAccessor;
use serde_json::json;
use std::{env, num::NonZeroUsize, path::Path, sync::Arc, time::Instant};

#[cfg(feature = "allocations")]
#[global_allocator]
static GLOBAL: &stats_alloc::StatsAlloc<std::alloc::System> = &stats_alloc::INSTRUMENTED_SYSTEM;

struct AllocationRegion {
    #[cfg(feature = "allocations")]
    start: stats_alloc::Stats,
}
impl AllocationRegion {
    fn new() -> Self {
        Self {
            #[cfg(feature = "allocations")]
            start: GLOBAL.stats(),
        }
    }
    fn record(&self, _output: &mut serde_json::Value) {
        #[cfg(feature = "allocations")]
        {
            let stats = GLOBAL.stats() - self.start;
            _output["allocation_calls"] = json!(stats.allocations);
            _output["reallocation_calls"] = json!(stats.reallocations);
            _output["allocated_bytes"] = json!(stats.bytes_allocated);
            _output["deallocated_bytes"] = json!(stats.bytes_deallocated);
        }
    }
}

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("positive setting")
}
fn byte_options() -> ByteReadOptions {
    ByteReadOptions {
        io_concurrency: nz(env::var("OAA_BENCH_IO_CONCURRENCY")
            .map_or(1, |v| v.parse().expect("integer I/O concurrency"))),
        io_chunk_size: env::var("OAA_BENCH_IO_CHUNK_SIZE")
            .ok()
            .map(|v| nz(v.parse().expect("integer I/O chunk size"))),
    }
}
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}
fn prepare(root: &Path, rows: usize) -> std::result::Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(root)?;
    let text = "benchmark payload with repeated text and UTF-8 你好. ".repeat(4);
    let mut csv = String::new();
    let mut jsonl = String::new();
    let mut xml = String::from("<rows>");
    let mut atif = String::new();
    let mut warc = String::new();
    for id in 0..rows {
        csv.push_str(&format!("{id},{text}\n"));
        jsonl.push_str(&format!("{}\n", json!({"id": id, "text": text})));
        xml.push_str(&format!("<row><id>{id}</id><text>{text}</text></row>"));
        atif.push_str(&format!("{}\n", json!({"schema_version":"ATIF-v1.8", "trajectory_id":id.to_string(), "agent":{"name":"benchmark","version":"1"}, "steps":[{"step_id":1,"source":"user","message":text}]})));
        warc.push_str(&format!("WARC/1.1\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-{id:012}>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nWARC-Target-URI: https://example.com/{id}\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n{text}\r\n\r\n", text.len()));
    }
    xml.push_str("</rows>");
    for (name, data) in [
        ("csv", csv),
        ("json", jsonl),
        ("xml", xml),
        ("atif", atif),
        ("warc", warc),
    ] {
        std::fs::write(root.join(name), data)?;
    }
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from_iter_values(0..rows as u64)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|_| text.as_str()),
            )),
        ],
    )?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(4096))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(root.join("parquet"))?,
        schema(),
        Some(props),
    )?;
    writer.write(&batch)?;
    writer.close()?;
    std::fs::write(root.join("rows"), rows.to_string())?;
    Ok(())
}

#[derive(Clone)]
struct Memory(Bytes);
#[async_trait]
impl Accessor for Memory {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(MemoryReader {
            data: self.0.clone(),
            offset: 0,
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.0.len() as u64),
                version: None,
            },
        }))
    }
}

impl ByteAccessor for Memory {}
struct MemoryReader {
    data: Bytes,
    offset: usize,
    metadata: ByteMetadata,
}
#[async_trait]
impl Reader for MemoryReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            read_parallel: false,
            list: false,
        }
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.offset == self.data.len() {
            return Ok(None);
        }
        let end = (self.offset + 65536).min(self.data.len());
        let bytes = self.data.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}
#[async_trait]
impl ByteReader for MemoryReader {
    async fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<Bytes> {
        validate_byte_range(&range, self.data.len() as u64, "bench_range")?;
        Ok(self.data.slice(range.start as usize..range.end as usize))
    }
}

#[derive(Clone)]
enum Storage {
    Memory(Memory),
    OpenDal(OpenDalAccessor),
    Connection(open_arrow_access_service::common::Connection),
}
#[async_trait]
impl Accessor for Storage {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        match self {
            Self::Memory(x) => x.capability(),
            Self::OpenDal(x) => x.capability(),
            Self::Connection(x) => x.capability(),
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        match self {
            Self::Memory(x) => x.open(request).await,
            Self::OpenDal(x) => x.open(request).await,
            Self::Connection(x) => {
                let url = x.origin().join(request.object.locator()).unwrap();
                x.open(url.as_str(), request.options).await
            }
        }
    }
}

async fn consume(mut reader: Box<dyn RecordReader>, start: Instant) -> Result<serde_json::Value> {
    let opened = start.elapsed().as_secs_f64();
    let mut first = None;
    let mut rows = 0;
    let mut batches = 0;
    let mut max_batch_bytes = 0;
    while let Some(batch) = reader.next().await? {
        first.get_or_insert_with(|| start.elapsed().as_secs_f64());
        rows += batch.num_rows();
        batches += 1;
        max_batch_bytes = max_batch_bytes.max(batch.get_array_memory_size());
        std::hint::black_box(&batch);
    }
    Ok(
        json!({"seconds":start.elapsed().as_secs_f64(), "open_seconds":opened, "first_batch_seconds":first, "rows":rows, "batches":batches, "max_batch_arrow_bytes":max_batch_bytes}),
    )
}

struct Settings<'a> {
    mode: &'a str,
    workers: usize,
    files: usize,
    batch: usize,
    projection: Option<Vec<String>>,
}
async fn records<F>(
    storage: Storage,
    format: F,
    name: &str,
    s: &Settings<'_>,
) -> Result<serde_json::Value>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
    FileAccessor<Storage, F>: ScanPlanner<Request = ReadRequest>,
{
    let files = FileSetAccessor::new(FileAccessor::from_format(storage, format));
    let request = FileSetRequest::new((0..s.files).map(|_| name))
        .with_options(ReadOptions {
            batch_size: nz(s.batch),
            projection: s.projection.clone(),
        })
        .with_byte_options(byte_options());
    let allocations = AllocationRegion::new();
    let start = Instant::now();
    let reader = match s.mode {
        "sequential" => files.open(request).await?,
        "files" => {
            files
                .layer(ParallelReadLayer::new(nz(s.workers), nz(2)))
                .open(request)
                .await?
        }
        "native" => {
            files
                .with_file_planning()
                .layer(ParallelReadLayer::new(nz(s.workers), nz(2)))
                .open(request)
                .await?
        }
        _ => panic!("unknown mode"),
    };
    let mut output = consume(reader, start).await?;
    allocations.record(&mut output);
    Ok(output)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        return Err("usage: prepare ROOT ROWS | provider NAME ITERATIONS | read ROOT FORMAT BACKEND MODE WORKERS FILES BATCH [PROJECTION]".into());
    }
    if args[1] == "prepare" {
        prepare(Path::new(&args[2]), args[3].parse()?)?;
        return Ok(());
    }
    if args[1] == "provider" {
        return providers::run(&args[2], args[3].parse()?).await;
    }
    if args[1] != "read" || args.len() < 9 {
        return Err("invalid read arguments".into());
    }
    opendal::install_default();
    // read ROOT FORMAT BACKEND MODE WORKERS FILES BATCH [PROJECTION comma-separated]
    let root = Path::new(&args[2]);
    let name = &args[3];
    let backend = &args[4];
    let storage = match backend.as_str() {
        "memory" => Storage::Memory(Memory(
            std::fs::read(root.join(if name == "bytes" { "json" } else { name }))?.into(),
        )),
        "fs" => Storage::OpenDal(OpenDalAccessor::new(opendal::Operator::new(
            opendal::services::Fs::default().root(root.to_str().unwrap()),
        )?)),
        "http" => Storage::OpenDal(OpenDalAccessor::new(opendal::Operator::new(
            opendal_service_http::Http::default().endpoint(&env::var("OAA_BENCH_ENDPOINT")?),
        )?)),
        "connection" => Storage::Connection(open_arrow_access_service::common::Connection::new(
            &env::var("OAA_BENCH_ENDPOINT")?,
        )?),
        "s3-fixture" => Storage::OpenDal(OpenDalAccessor::new(opendal::Operator::new(
            opendal_service_s3::S3::default()
                .endpoint(&env::var("OAA_BENCH_ENDPOINT")?)
                .region("us-east-1")
                .bucket("oaa-bench")
                .access_key_id("benchmark-only")
                .secret_access_key("benchmark-only")
                .disable_config_load()
                .disable_ec2_metadata(),
        )?)),
        _ => panic!("unknown backend"),
    };
    let s = Settings {
        mode: &args[5],
        workers: args[6].parse()?,
        files: args[7].parse()?,
        batch: args[8].parse()?,
        projection: args
            .get(9)
            .map(|x| x.split(',').map(str::to_owned).collect()),
    };
    let mut output = if name == "bytes" {
        let start = Instant::now();
        let mut total = 0;
        for _ in 0..s.files {
            let mut r = storage
                .open(ByteReadRequest::new("json").with_options(byte_options()))
                .await?;
            while let Some(bytes) = r.next().await? {
                total += bytes.len();
                std::hint::black_box(bytes);
            }
        }
        json!({"seconds":start.elapsed().as_secs_f64(),"bytes":total})
    } else {
        match name.as_str() {
            "csv" => records(storage, CsvFileFormat::new(schema()), name, &s).await?,
            "json" => records(storage, JsonFormat::new(schema()), name, &s).await?,
            "parquet" => records(storage, ParquetFormat, name, &s).await?,
            "atif" => records(storage, AtifFormat::JsonLines, name, &s).await?,
            "warc" => records(storage, WarcArrowFormat, name, &s).await?,
            "xml" => {
                records(
                    storage,
                    XmlFormat::new("/rows/row").with_schema(XmlSchema::new(vec![
                        XmlField::new("id", "id", DataType::Int64, false),
                        XmlField::new("text", "text", DataType::Utf8, false),
                    ])?),
                    name,
                    &s,
                )
                .await?
            }
            _ => panic!("unknown format"),
        }
    };
    if name != "bytes" {
        let expected: usize = std::fs::read_to_string(root.join("rows"))?.parse()?;
        assert_eq!(
            output["rows"].as_u64().unwrap() as usize,
            expected * s.files
        );
    }
    output["format"] = json!(name);
    output["backend"] = json!(backend);
    output["mode"] = json!(s.mode);
    output["workers"] = json!(s.workers);
    output["files"] = json!(s.files);
    output["batch_size"] = json!(s.batch);
    println!("{output}");
    Ok(())
}
