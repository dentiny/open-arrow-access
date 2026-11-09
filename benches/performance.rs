mod pipelines;
mod providers;
// Deterministic, process-isolated benchmarks. See docs/performance.md.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ListRequest, ReadOptions, ReadRequest, RecordReader,
};
use open_arrow_access_file_format::{
    atif::{AtifAccessor, AtifFormat},
    csv::CsvAccessor,
    json::JsonAccessor,
    parquet::ParquetAccessor,
    warc::WarcAccessor,
    xml::{XmlAccessor, XmlField, XmlSchema},
};
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};
use open_arrow_access_test_support::{MemoryAccessor, http::HttpFixture};
use std::{hint::black_box, num::NonZeroUsize, sync::Arc, time::Instant};

async fn drain(mut reader: Box<dyn RecordReader>) -> usize {
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
        black_box(batch);
    }
    rows
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    let case = args.get(1).map(String::as_str).unwrap_or("json");
    let iterations: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(10);
    assert!(iterations > 0);
    if let Some(name) = case.strip_prefix("service-") {
        if matches!(
            name,
            "pmc" | "gutenberg" | "courtlistener" | "meca" | "arxiv-bulk" | "commoncrawl"
        ) {
            pipelines::run(name, iterations).await;
        } else {
            providers::run(name, iterations).await;
        }
        return;
    }
    let chunk: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(65536);
    let batch: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(1024);
    let projected = case.ends_with("-projected");
    let ranged = case.ends_with("-ranges");
    let name = case
        .trim_end_matches("-projected")
        .trim_end_matches("-ranges");
    let n = if name == "atif" { 4096 } else { 32768 };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let text = "benchmark payload ".repeat(16);
    let data: Vec<u8> = match name {
        "json" => format!("{{\"id\":42,\"text\":\"{text}\"}}\n").repeat(n).into_bytes(),
        "csv" => format!("42,{text}\n").repeat(n).into_bytes(),
        "xml" | "xml-infer" => format!("<root>{}</root>", format!("<row><id>42</id><text>{text}</text></row>").repeat(n)).into_bytes(),
        "atif" => format!("{{\"schema_version\":\"ATIF-v1.8\",\"trajectory_id\":\"benchmark\",\"agent\":{{\"name\":\"bench\",\"version\":\"1\"}},\"steps\":[{{\"step_id\":1,\"source\":\"user\",\"message\":\"{text}\"}}]}}\n").repeat(n).into_bytes(),
        "warc" => format!("WARC/1.0\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: {}\r\n\r\n{text}\r\n\r\n", text.len()).repeat(n).into_bytes(),
        "parquet" => {
            let record = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from_iter_values(0..n as i64)), Arc::new(StringArray::from_iter_values(std::iter::repeat_n(text.as_str(), n)))]).unwrap();
            let mut out = Vec::new();
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut out, schema.clone(), None).unwrap();
            writer.write(&record).unwrap(); writer.close().unwrap(); out
        },
        _ => vec![b'x'; 8 * 1024 * 1024],
    };
    let memory = MemoryAccessor::new(&data, chunk).with_ranges();
    let fixture = HttpFixture::new();
    if matches!(name, "http" | "hf" | "connection") {
        let body = open_arrow_access_core::Bytes::copy_from_slice(&data);
        fixture.handle(move |request| Ok(serve_file(&request, body.clone())));
    }
    let temp = tempfile::tempdir().unwrap();
    if name == "fs" {
        std::fs::write(temp.path().join("data"), &data).unwrap();
    }
    let fs = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(temp.path()))
        .with_chunk_size(NonZeroUsize::new(chunk).unwrap());
    let http =
        open_arrow_access_service::commoncrawl::create_http_accessor(fixture.endpoint()).unwrap();
    let hf = if name == "hf" {
        fixture.respond_for(http::Method::POST, "/api/datasets/bench/data/paths-info/main", http::StatusCode::OK, serde_json::to_vec(&serde_json::json!([{"type":"file","path":"data","size":data.len(),"oid":"benchmark"}])).unwrap());
        Some(
            open_arrow_access_service::huggingface::HuggingFaceService::new(
                open_arrow_access_service::huggingface::HuggingFaceOptions::default()
                    .with_endpoint(fixture.endpoint())
                    .with_token("benchmark-only"),
            )
            .await
            .unwrap()
            .repository(
                open_arrow_access_service::huggingface::HuggingFaceRepository::new("bench/data"),
            )
            .await
            .unwrap(),
        )
    } else {
        None
    };
    let connection = open_arrow_access_service::common::Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(opendal::OperationContext::new());
    let named = NamedFileAccessor::new(
        memory.clone(),
        (0..if name.starts_with("named-") {
            100_000
        } else {
            0
        })
            .map(|i| (format!("file-{i}"), "data".into()))
            .collect(),
    )
    .unwrap();
    let options = ReadOptions {
        batch_size: NonZeroUsize::new(batch).unwrap(),
        projection: projected.then(|| {
            vec![
                if name == "atif" {
                    "trajectory_id"
                } else {
                    "id"
                }
                .into(),
            ]
        }),
    };
    let request = ReadRequest::new("data").with_options(options);
    let xml_schema = XmlSchema::new(vec![
        XmlField::new("id", "id", DataType::Int64, false),
        XmlField::new("text", "text", DataType::Utf8, false),
    ])
    .unwrap();
    // Setup is deliberately outside the timed region. A separate process per sample
    // gives a whole-process RSS/CPU measurement, including the documented fixtures.
    let start = Instant::now();
    let mut total = 0;
    let mut http_requests = 0;
    for _ in 0..iterations {
        let count = match name {
            "json" => {
                drain(
                    JsonAccessor::new(memory.clone(), schema.clone())
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "csv" => {
                drain(
                    CsvAccessor::new(memory.clone(), schema.clone())
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "atif" => {
                drain(
                    AtifAccessor::new(memory.clone(), AtifFormat::JsonLines)
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "xml" => {
                drain(
                    XmlAccessor::new(memory.clone(), "/root/row")
                        .with_schema(xml_schema.clone())
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "xml-infer" => {
                drain(
                    XmlAccessor::new(memory.clone(), "/root/row")
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "parquet" => {
                drain(
                    ParquetAccessor::new(memory.clone())
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "warc" => {
                let mut r = WarcAccessor::new(memory.clone())
                    .open(ByteReadRequest::new("data"))
                    .await
                    .unwrap();
                let mut count = 0;
                while let Some(record) = r.next().await.unwrap() {
                    black_box(record);
                    count += 1;
                }
                count
            }
            "named-list" | "named-first" => {
                let mut s = named.list(ListRequest::new("")).await.unwrap();
                let mut count = 0;
                while let Some(object) = s.try_next().await.unwrap() {
                    black_box(object);
                    count += 1;
                    if name == "named-first" {
                        break;
                    }
                }
                count
            }
            "fs" | "http" | "hf" | "connection" => {
                let mut r = match name {
                    "fs" => fs.open(ByteReadRequest::new("data")).await.unwrap(),
                    "hf" => hf
                        .as_ref()
                        .unwrap()
                        .open(ByteReadRequest::new("data"))
                        .await
                        .unwrap(),
                    "http" => http.open(ByteReadRequest::new("data")).await.unwrap(),
                    _ => connection
                        .open(&format!("{}/data", fixture.endpoint()), Default::default())
                        .await
                        .unwrap(),
                };
                let mut count = 0;
                if ranged {
                    for bytes in r
                        .read_ranges((0..1024).map(|i| (i * 8192)..(i * 8192 + 4096)).collect())
                        .await
                        .unwrap()
                    {
                        count += bytes.len();
                        black_box(bytes);
                    }
                } else {
                    while let Some(bytes) = r.next().await.unwrap() {
                        count += bytes.len();
                        black_box(bytes);
                    }
                }
                count
            }
            _ => panic!("unknown benchmark {case}"),
        };
        let expected = match name {
            "named-first" => 1,
            "named-list" => 100_000,
            "fs" | "http" | "hf" | "connection" => {
                if ranged {
                    4 * 1024 * 1024
                } else {
                    data.len()
                }
            }
            _ => n,
        };
        assert_eq!(count, expected);
        total += count;
        http_requests += fixture.requests().len();
        fixture.clear_requests();
        // Test observations must not grow across iterations.
        memory.observations.requests.lock().unwrap().clear();
        memory.observations.ranges.lock().unwrap().clear();
    }
    let seconds = start.elapsed().as_secs_f64();
    let obs = &memory.observations;
    println!(
        "{}",
        serde_json::json!({"case":case,"iterations":iterations,"chunk_bytes":chunk,"batch_rows":batch,"input_bytes":data.len(),"units":total,"seconds":seconds,"mib_per_second":data.len() as f64*iterations as f64/1048576.0/seconds,"sequential_calls":obs.reads.load(std::sync::atomic::Ordering::Relaxed),"range_calls":obs.range_reads.load(std::sync::atomic::Ordering::Relaxed),"http_requests":http_requests})
    );
}

// The contract fixtures fragment into three-byte chunks to stress parsers. Bulk
// throughput uses realistic 64 KiB frames, generated lazily to bound staging.
fn serve_file(
    request: &http::Request<opendal::Buffer>,
    bytes: open_arrow_access_core::Bytes,
) -> http::Response<opendal::HttpBody> {
    let length = bytes.len();
    let head = request.method() == http::Method::HEAD;
    let mut response = http::Response::builder().status(200);
    let mut body = if head {
        open_arrow_access_core::Bytes::new()
    } else if let Some(range) = request.headers().get("range") {
        let (start, end) = range
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start: usize = start.parse().unwrap();
        let end = if end.is_empty() {
            length
        } else {
            end.parse::<usize>().unwrap() + 1
        };
        response = response.status(206).header(
            "content-range",
            format!("bytes {}-{}/{}", start, end - 1, length),
        );
        bytes.slice(start..end)
    } else {
        bytes
    };
    let size = body.len();
    response
        .header("content-length", if head { length } else { size })
        .body(opendal::HttpBody::new(
            futures::stream::iter(std::iter::from_fn(move || {
                if body.is_empty() {
                    None
                } else {
                    let part = body.split_to(body.len().min(65536));
                    Some(Ok(opendal::Buffer::from(part)))
                }
            })),
            Some(size as u64),
        ))
        .unwrap()
}
