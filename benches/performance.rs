//! Deterministic end-to-end workloads. Run scripts/benchmark.py for CPU/RSS measurements.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ReadOptions, ReadRequest, RecordReader,
};
use open_arrow_access_file_format::{
    atif::{AtifAccessor, AtifFormat},
    csv::CsvAccessor,
    json::JsonAccessor,
    parquet::ParquetAccessor,
    warc::WarcAccessor,
    xml::{XmlAccessor, XmlField, XmlSchema},
};
use open_arrow_access_service::common::Connection;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fs_operator, http::HttpFixture};
use parquet::arrow::ArrowWriter;
use std::{hint::black_box, num::NonZeroUsize, sync::Arc, time::Instant};

const CASES: &[&str] = &[
    "csv",
    "csv/projected",
    "json",
    "json/projected",
    "parquet",
    "parquet/projected",
    "atif",
    "atif/document",
    "atif/unknown",
    "atif/unknown/projected",
    "atif/projected",
    "xml",
    "xml/projected",
    "xml/inferred",
    "warc",
    "storage/fs",
    "storage/http",
    "storage/http-known-length",
    "storage/http-ranges",
    "service/metadata",
    "service/body",
    "service/download",
    "lines/fragmented",
];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

async fn drain(mut reader: Box<dyn RecordReader>) -> usize {
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
        black_box(&batch);
    }
    rows
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let case = std::env::args().nth(1).unwrap_or_else(|| "--list".into());
    if case == "--bench" {
        // Cargo passes --bench even for a custom harness. Isolate every case in
        // its own process, matching the documented Python runner's lifecycle.
        for case in CASES {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg(case)
                .status()
                .unwrap();
            assert!(status.success(), "benchmark {case} failed");
        }
        return;
    }
    if case == "--list" {
        for case in CASES {
            println!("{case}");
        }
        return;
    }
    assert!(CASES.contains(&case.as_str()), "unknown benchmark {case}");
    let rows: usize = std::env::var("BENCH_ROWS")
        .unwrap_or_else(|_| "20000".into())
        .parse()
        .unwrap();
    let rounds: usize = std::env::var("BENCH_ROUNDS")
        .unwrap_or_else(|_| "5".into())
        .parse()
        .unwrap();
    let chunk: usize = std::env::var("BENCH_CHUNK")
        .unwrap_or_else(|_| "65536".into())
        .parse()
        .unwrap();
    let batch: usize = std::env::var("BENCH_BATCH")
        .unwrap_or_else(|_| "1024".into())
        .parse()
        .unwrap();
    assert!(rows > 0 && rounds > 0 && chunk > 0 && batch > 0);
    let text = "payload & unicode 你好 ".repeat(8);
    let mut input = Vec::new();
    match case.split('/').next().unwrap() {
        "csv" => {
            for i in 0..rows {
                input.extend_from_slice(format!("{i},{text}\n").as_bytes());
            }
        }
        "json" => {
            for i in 0..rows {
                input.extend_from_slice(
                    format!(
                        "{{\"id\":{i},\"text\":{}}}\n",
                        serde_json::to_string(&text).unwrap()
                    )
                    .as_bytes(),
                );
            }
        }
        "atif" => {
            let mut document: serde_json::Value =
                serde_json::from_str(include_str!("../testdata/atif/trajectory.json")).unwrap();
            if case.contains("unknown") {
                document["unmodeled_payload"] = serde_json::Value::String("x".repeat(4096));
            }
            if case == "atif/document" {
                document["steps"] = serde_json::Value::Array(
                    (1..=rows)
                        .map(|i| serde_json::json!({"step_id":i,"source":"user","message":text}))
                        .collect(),
                );
                input = serde_json::to_vec(&document).unwrap();
            } else {
                let line = format!("{}\n", serde_json::to_string(&document).unwrap());
                input = line.repeat(rows).into_bytes();
            }
        }
        "xml" => {
            input.extend_from_slice(b"<rows>");
            for i in 0..rows {
                input.extend_from_slice(
                    format!(
                        "<row><id>{i}</id><text>{}</text></row>",
                        text.replace('&', "&amp;")
                    )
                    .as_bytes(),
                );
            }
            input.extend_from_slice(b"</rows>");
        }
        "parquet" => {
            let data = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|_| text.as_str()),
                    )),
                ],
            )
            .unwrap();
            let mut writer = ArrowWriter::try_new(&mut input, schema(), None).unwrap();
            writer.write(&data).unwrap();
            writer.close().unwrap();
        }
        "warc" => {
            for i in 0..rows {
                input.extend_from_slice(format!("WARC/1.1\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:uuid:{i:032x}>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: {}\r\n\r\n{text}\r\n\r\n", text.len()).as_bytes());
            }
        }
        "lines" => {
            input = ("x".repeat(4095) + "\n").repeat(rows).into_bytes();
        }
        "service" if case == "service/metadata" => {
            let entries: Vec<_> = (0..rows)
                .map(|i| serde_json::json!({"id":i,"text":text}))
                .collect();
            input = serde_json::to_vec(&serde_json::json!({"entries":entries})).unwrap();
        }
        _ => input = vec![42; rows * 1024],
    }
    let input_len = input.len();
    let memory = MemoryAccessor::new(
        &input,
        if case == "lines/fragmented" {
            257
        } else {
            chunk
        },
    )
    .with_ranges();
    let dir = tempfile::tempdir().unwrap();
    let fixture = HttpFixture::new();
    // All fixture and payload construction happens before timed iterations.
    std::fs::write(dir.path().join("data"), &input).unwrap();
    let body = open_arrow_access_core::Bytes::copy_from_slice(&input);
    fixture.handle(move |request| {
        use open_arrow_access_storage::opendal::{Buffer, HttpBody};
        let len = body.len();
        let mut response = http::Response::builder();
        let bytes = if request.method() == http::Method::HEAD {
            response = response.header("content-length", len);
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
                len
            } else {
                (end.parse::<usize>().unwrap() + 1).min(len)
            };
            response = response
                .status(206)
                .header(
                    "content-range",
                    format!("bytes {}-{}/{}", start, end - 1, len),
                )
                .header("content-length", end - start);
            body.slice(start..end)
        } else {
            response = response.header("content-length", len);
            body.clone()
        };
        let size = bytes.len() as u64;
        Ok(response
            .body(HttpBody::new(
                futures::stream::iter(Buffer::from(bytes).chunks(16384).map(Ok)),
                Some(size),
            ))
            .unwrap())
    });
    let connection = Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(open_arrow_access_storage::opendal::OperationContext::new());
    let http_operator = open_arrow_access_storage::opendal::Operator::new(
        open_arrow_access_storage::opendal::services::Http::default().endpoint(fixture.endpoint()),
    )
    .unwrap();
    let storage = if case == "storage/fs" {
        OpenDalAccessor::new(fs_operator(dir.path()))
    } else {
        OpenDalAccessor::new(http_operator)
    };
    let storage = if case == "storage/http-known-length" {
        storage.with_content_lengths([("data".to_owned(), input_len as u64)])
    } else {
        storage
    };
    let projected = case.ends_with("/projected");
    let request = ReadRequest::new("data").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(batch).unwrap(),
        projection: projected.then(|| {
            vec![if case.starts_with("atif") {
                "trajectory_id".into()
            } else {
                "id".into()
            }]
        }),
    });
    for iteration in 0..=rounds {
        fixture.clear_requests();
        let started = Instant::now();
        let units = match case.split('/').next().unwrap() {
            "csv" => {
                drain(
                    CsvAccessor::new(memory.clone(), schema())
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "json" => {
                drain(
                    JsonAccessor::new(memory.clone(), schema())
                        .open(request.clone())
                        .await
                        .unwrap(),
                )
                .await
            }
            "atif" => {
                drain(
                    AtifAccessor::new(
                        memory.clone(),
                        if case == "atif/document" {
                            AtifFormat::Json
                        } else {
                            AtifFormat::JsonLines
                        },
                    )
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
            "xml" => {
                let accessor = XmlAccessor::new(memory.clone(), "/rows/row");
                let accessor = if case.ends_with("/inferred") {
                    accessor
                } else {
                    accessor.with_schema(
                        XmlSchema::new(vec![
                            XmlField::new("id", "id", DataType::Int64, false),
                            XmlField::new("text", "text", DataType::Utf8, false),
                        ])
                        .unwrap(),
                    )
                };
                drain(accessor.open(request.clone()).await.unwrap()).await
            }
            "warc" => {
                let mut reader = WarcAccessor::new(memory.clone())
                    .open(ByteReadRequest::new("data"))
                    .await
                    .unwrap();
                let mut count = 0;
                while let Some(record) = reader.next().await.unwrap() {
                    black_box(record);
                    count += 1;
                }
                count
            }
            "lines" => {
                let lines = open_arrow_access_core::read_lines(
                    memory.open(ByteReadRequest::new("data")).await.unwrap(),
                    None,
                    "bench",
                );
                futures::pin_mut!(lines);
                let mut count = 0;
                while let Some(line) = lines.try_next().await.unwrap() {
                    black_box(line);
                    count += 1;
                }
                count
            }
            "service" if case == "service/body" => {
                let bytes = connection
                    .bytes(
                        connection.url("data", &[]).unwrap(),
                        "application/octet-stream",
                    )
                    .await
                    .unwrap();
                assert_eq!(bytes.len(), input_len);
                black_box(bytes);
                input_len
            }
            "service" if case == "service/metadata" => {
                let document = connection.json("metadata", &[]).await.unwrap();
                black_box(document);
                rows
            }
            _ => {
                let mut reader = if case == "service/download" {
                    connection
                        .open(
                            &format!("{}/data", fixture.endpoint()),
                            ByteReadOptions::default(),
                        )
                        .await
                        .unwrap()
                } else {
                    storage.open(ByteReadRequest::new("data")).await.unwrap()
                };
                let mut bytes = 0;
                if case.ends_with("-ranges") {
                    let ranges = (0..rows.min(8192))
                        .map(|i| (i * 1024) as u64..(i * 1024 + 64) as u64)
                        .collect();
                    for data in reader.read_ranges(ranges).await.unwrap() {
                        bytes += data.len();
                        black_box(data);
                    }
                    assert_eq!(bytes, rows.min(8192) * 64);
                } else {
                    while let Some(data) = reader.next().await.unwrap() {
                        bytes += data.len();
                        black_box(data);
                    }
                    assert_eq!(bytes, input_len);
                }
                bytes
            }
        };
        let elapsed = started.elapsed().as_secs_f64();
        if matches!(
            case.split('/').next().unwrap(),
            "csv" | "json" | "atif" | "xml" | "parquet" | "warc" | "lines"
        ) {
            assert_eq!(units, if case == "atif/document" { 1 } else { rows });
        }
        if iteration > 0 {
            println!(
                "{}",
                serde_json::json!({"case":case,"iteration":iteration,"seconds":elapsed,"input_bytes":input_len,"units":units,"http_requests":fixture.requests().len(),"batch":batch,"chunk":chunk})
            );
        }
    }
}
