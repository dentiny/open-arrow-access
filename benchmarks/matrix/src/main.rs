//! Deterministic local workloads. Run each case in a separate process via run.py.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ReadOptions, ReadRequest, RecordBatchBuilder, RecordReader,
};
use open_arrow_access_file_format::{
    atif::{AtifAccessor, AtifFormat},
    csv::CsvAccessor,
    json::JsonAccessor,
    parquet::ParquetAccessor,
    warc::WarcAccessor,
    xml::{XmlAccessor, XmlField, XmlSchema},
};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fs_operator, http::HttpFixture};
use serde_json::json;
use std::{
    hint::black_box,
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

mod providers;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]))
}
fn request(projected: bool) -> ReadRequest {
    ReadRequest::new("data").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(1024).unwrap(),
        projection: projected.then(|| vec!["id".into()]),
    })
}
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
    let case = std::env::args().nth(1).unwrap_or_else(|| "list".into());
    let cases = [
        "csv",
        "csv-project",
        "json",
        "json-project",
        "atif",
        "atif-project",
        "xml",
        "xml-project",
        "xml-infer",
        "parquet",
        "parquet-project",
        "warc",
        "fs-sequential",
        "fs-ranges",
        "http-sequential",
        "http-small-fragments",
        "http-ranges",
        "metadata-builder",
        "metadata-builder-wide",
        "chunks-short",
        "chunks-retained",
        "http-fragmented",
    ];
    if case == "list" {
        for name in cases {
            println!("{name}");
        }
        providers::list();
        return;
    }
    let seconds: f64 = std::env::var("BENCH_SECONDS")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap();
    assert!(seconds.is_finite() && seconds > 0.0);
    let duration = Duration::from_secs_f64(seconds);
    if case.starts_with("provider-") {
        providers::run(&case, duration).await;
        return;
    }
    assert!(cases.contains(&case.as_str()), "unknown benchmark: {case}");
    let n = 16384;
    let payload = "abcdefghijklmnop".repeat(16);
    let data: Vec<u8> = match case.as_str() {
        "csv" | "csv-project" => (0..n).map(|i| format!("{i},{payload}\n")).collect::<String>().into_bytes(),
        "json" | "json-project" => (0..n).map(|i| format!("{{\"id\":{i},\"text\":\"{payload}\"}}\n")).collect::<String>().into_bytes(),
        "xml" | "xml-project" | "xml-infer" => format!("<rows>{}</rows>", (0..n).map(|i| format!("<row><id>{i}</id><text>{payload}</text></row>")).collect::<String>()).into_bytes(),
        "atif" | "atif-project" => {
            let line = json!({"schema_version":"ATIF-v1.6", "session_id":"benchmark", "agent":{"name":"bench", "version":"1"}, "steps":[{"step_id":1,"source":"user","message":payload}]}).to_string() + "\n";
            line.repeat(n).into_bytes()
        },
        "parquet" | "parquet-project" => {
            let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from_iter_values(0..n as i64)), Arc::new(StringArray::from(vec![payload.as_str();n]))]).unwrap();
            let mut bytes = Vec::new();
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema(), None).unwrap();
            writer.write(&batch).unwrap(); writer.close().unwrap(); bytes
        },
        "warc" => format!("WARC/1.0\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:bench>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: {}\r\n\r\n{payload}\r\n\r\n", payload.len()).repeat(n).into_bytes(),
        _ if case.starts_with("fs-") || case.starts_with("http-") => vec![42; 16 * 1024 * 1024],
        _ => Vec::new(),
    };
    let chunk_size = std::env::var("BENCH_CHUNK_SIZE")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(64 * 1024);
    let memory = MemoryAccessor::new(&data, chunk_size).with_ranges();
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("data"), &data).unwrap();
    let http_fragment_size = std::env::var("BENCH_HTTP_FRAGMENT_SIZE")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(if case == "http-small-fragments" {
            7000
        } else {
            64 * 1024
        });
    let fixture = HttpFixture::new();
    if case.starts_with("http-") && case != "http-fragmented" {
        let data = open_arrow_access_core::Bytes::copy_from_slice(&data);
        fixture.handle(move |request| {
            let head = request.method() == http::Method::HEAD;
            let bytes = if let Some(range) = request.headers().get("range") {
                let (start, end) = range
                    .to_str()
                    .unwrap()
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                let start: usize = start.parse().unwrap();
                let end = if end.is_empty() {
                    data.len()
                } else {
                    end.parse::<usize>().unwrap() + 1
                };
                data.slice(start..end)
            } else {
                data.clone()
            };
            let size = bytes.len() as u64;
            let mut response =
                open_arrow_access_test_support::http::file_response(&request, data.clone());
            if !head {
                *response.body_mut() = opendal::HttpBody::new(
                    futures::stream::iter(
                        opendal::Buffer::from(bytes)
                            .chunks(http_fragment_size)
                            .map(Ok),
                    ),
                    Some(size),
                );
            }
            Ok(response)
        });
    }
    if case == "http-fragmented" {
        use open_arrow_access_core::Bytes;
        let parts: Vec<_> = (0..256).map(|_| Bytes::from(vec![42; 64 * 1024])).collect();
        fixture.handle(move |_| {
            let buffer: opendal::Buffer = parts.clone().into_iter().collect();
            Ok(http::Response::builder()
                .status(200)
                .header("content-length", 16 * 1024 * 1024)
                .body(opendal::HttpBody::new(
                    futures::stream::iter([Ok(buffer)]),
                    Some(16 * 1024 * 1024),
                ))
                .unwrap())
        });
    }
    let connection = open_arrow_access_service::common::Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(opendal::OperationContext::new());
    let fs = OpenDalAccessor::new(fs_operator(temp.path()));
    let http = OpenDalAccessor::new(
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(fixture.endpoint()))
            .unwrap(),
    );
    let projected = case.ends_with("-project");
    let xml_schema = XmlSchema::new(vec![
        XmlField::new("id", "id", DataType::Int64, false),
        XmlField::new("text", "text", DataType::Utf8, true),
    ])
    .unwrap();
    let fields = if case == "metadata-builder-wide" {
        128
    } else {
        8
    };
    let metadata_schema = Arc::new(Schema::new(
        (0..fields)
            .map(|i| Field::new(format!("f{i}"), DataType::UInt64, false))
            .collect::<Vec<_>>(),
    ));
    let names: Vec<_> = (0..fields).map(|i| format!("f{i}")).collect();
    let mut elapsed = Duration::ZERO;
    let mut iterations = 0;
    let mut reads = 0;
    let mut range_reads = 0;
    let mut transferred = 0;
    let mut retained_capacity;
    // One untimed warmup, then time only open+consume; setup/fixture reset excluded.
    loop {
        memory.observations.requests.lock().unwrap().clear();
        memory.observations.ranges.lock().unwrap().clear();
        memory.observations.reads.store(0, Ordering::Relaxed);
        memory.observations.range_reads.store(0, Ordering::Relaxed);
        fixture.clear_requests();
        retained_capacity = 0;
        let started = Instant::now();
        let units = match case.as_str() {
            "csv" | "csv-project" => {
                drain(
                    CsvAccessor::new(memory.clone(), schema())
                        .open(request(projected))
                        .await
                        .unwrap(),
                )
                .await
            }
            "json" | "json-project" => {
                drain(
                    JsonAccessor::new(memory.clone(), schema())
                        .open(request(projected))
                        .await
                        .unwrap(),
                )
                .await
            }
            "xml" | "xml-project" => {
                drain(
                    XmlAccessor::new(memory.clone(), "/rows/row")
                        .with_schema(xml_schema.clone())
                        .open(request(projected))
                        .await
                        .unwrap(),
                )
                .await
            }
            "xml-infer" => {
                drain(
                    XmlAccessor::new(memory.clone(), "/rows/row")
                        .open(request(false))
                        .await
                        .unwrap(),
                )
                .await
            }
            "atif" | "atif-project" => {
                let mut req = request(false);
                if projected {
                    req.options.projection = Some(vec!["session_id".into()]);
                }
                drain(
                    AtifAccessor::new(memory.clone(), AtifFormat::JsonLines)
                        .open(req)
                        .await
                        .unwrap(),
                )
                .await
            }
            "parquet" | "parquet-project" => {
                drain(
                    ParquetAccessor::new(memory.clone())
                        .open(request(projected))
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
            "http-fragmented" => {
                let mut reader = connection
                    .open(
                        &format!("{}/fragmented", fixture.endpoint()),
                        Default::default(),
                    )
                    .await
                    .unwrap();
                let mut retained = Vec::new();
                let mut len = 0;
                while let Some(bytes) = reader.next().await.unwrap() {
                    len += bytes.len();
                    retained.push(bytes);
                }
                assert_eq!(len, 16 * 1024 * 1024);
                black_box(retained);
                n
            }
            "fs-sequential" | "http-sequential" | "http-small-fragments" => {
                let a = if case.starts_with("fs") { &fs } else { &http };
                let mut r = a.open(ByteReadRequest::new("data")).await.unwrap();
                let mut len = 0;
                while let Some(bytes) = r.next().await.unwrap() {
                    len += bytes.len();
                    black_box(bytes);
                }
                assert_eq!(len, data.len());
                n
            }
            "fs-ranges" | "http-ranges" => {
                let a = if case.starts_with("fs") { &fs } else { &http };
                let mut r = a.open(ByteReadRequest::new("data")).await.unwrap();
                let ranges: Vec<_> = (0..4096u64).map(|i| i * 4096..i * 4096 + 64).collect();
                let result = r.read_ranges(ranges).await.unwrap();
                assert_eq!(result.len(), 4096);
                assert!(result.iter().all(|v| v.len() == 64));
                black_box(result);
                n
            }
            "metadata-builder" | "metadata-builder-wide" => {
                let mut builder = RecordBatchBuilder::new(metadata_schema.clone(), None).unwrap();
                for i in 0..n {
                    builder
                        .push(|row| {
                            for name in &names {
                                row.append(name, i as u64)?;
                            }
                            Ok(())
                        })
                        .unwrap();
                }
                let batch = builder.finish().unwrap();
                black_box(batch);
                n
            }
            "chunks-short" | "chunks-retained" => {
                use futures::{TryStreamExt, io::Cursor};
                let mut retained = Vec::new();
                for _ in 0..if case == "chunks-retained" { 128 } else { 1 } {
                    let input = Cursor::new(vec![1u8; 64]);
                    let chunks = open_arrow_access_core::read_chunks(
                        input,
                        NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
                    );
                    futures::pin_mut!(chunks);
                    let mut len = 0;
                    while let Some(bytes) = chunks.try_next().await.unwrap() {
                        len += bytes.len();
                        let bytes = bytes.try_into_mut().unwrap();
                        retained_capacity += bytes.capacity();
                        retained.push(bytes.freeze());
                    }
                    assert_eq!(len, 64);
                }
                black_box(retained);
                n
            }
            _ => unreachable!(),
        };
        assert_eq!(units, n);
        let time = started.elapsed();
        if iterations > 0 {
            elapsed += time;
            reads += memory.observations.reads.load(Ordering::Relaxed);
            range_reads += memory.observations.range_reads.load(Ordering::Relaxed);
            transferred += fixture
                .requests()
                .iter()
                .filter_map(|r| r.range())
                .map(|r| r.len())
                .sum::<usize>();
        }
        iterations += 1;
        if elapsed >= duration {
            break;
        }
    }
    let iterations = iterations - 1;
    println!(
        "{}",
        json!({"case":case,"iterations":iterations,"wall_seconds":elapsed.as_secs_f64(),"ns_per_iteration":elapsed.as_nanos() as f64/iterations as f64,"input_bytes":data.len(),"retained_capacity_bytes":retained_capacity,"rows_per_iteration": if case.starts_with("fs-") || case.starts_with("http-") || case.starts_with("chunks-") {0} else {n},
        "logical_bytes_per_iteration": if case.ends_with("-ranges") {4096*64} else if case=="chunks-short" {64} else if case=="chunks-retained" {128*64} else {data.len()},"read_calls":reads,"range_calls":range_reads,"http_range_bytes":transferred,"http_note":"in-process transport; no socket or network latency"})
    );
}
