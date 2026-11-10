//! Deterministic throughput and I/O probes. See benchmarks/README.md.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::file_format::{
    atif::{AtifAccessor, AtifFormat},
    csv::CsvAccessor,
    json::JsonAccessor,
    parquet::ParquetAccessor,
    warc::WarcAccessor,
    xml::{XmlAccessor, XmlField, XmlSchema},
};
use open_arrow_access::{Accessor, ByteReadOptions, ByteReadRequest, ReadOptions, ReadRequest};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::MemoryAccessor;
mod transport;
use serde_json::json;
use std::{hint::black_box, num::NonZeroUsize, sync::Arc, time::Instant};
use transport::Transport;

const CASES: &[&str] = &[
    "csv",
    "csv-project",
    "json",
    "json-project",
    "parquet",
    "parquet-project",
    "xml",
    "xml-project",
    "xml-infer",
    "xml-nested",
    "xml-struct",
    "atif",
    "atif-document",
    "atif-project",
    "atif-tools",
    "atif-timestamp",
    "warc",
    "storage-sequential",
    "storage-range",
    "storage-scattered",
    "storage-adjacent",
    "lines",
    "service-body",
    "service-download",
    "service-metadata",
];
fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("positive option")
}
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

fn generate(case: &str, rows: usize, width: usize) -> Vec<u8> {
    let text = "x".repeat(width);
    match case {
        "csv" | "csv-project" => format!("42,{text}\n").repeat(rows).into_bytes(),
        "json" | "json-project" => format!("{{\"id\":42,\"text\":\"{text}\"}}\n").repeat(rows).into_bytes(),
        "xml" | "xml-project" | "xml-infer" => format!("<rows>{}</rows>", format!("<row><id>42</id><text>{text}</text></row>").repeat(rows)).into_bytes(),
        "xml-struct" => format!("<rows>{}</rows>", format!("<row><a><b id=\"42\"><text>{text}</text></b></a></row>").repeat(rows)).into_bytes(),
        "xml-nested" => format!("<rows>{}</rows>", format!("<row><a><b><id>42</id><text>{text}</text></b></a></row>").repeat(rows)).into_bytes(),
        "atif" | "atif-document" | "atif-project" | "atif-tools" | "atif-timestamp" => {
            let mut step = if case == "atif-tools" {
                json!({"step_id":1,"source":"agent","message":"tools",
                    "tool_calls": (0..width).map(|i| json!({"tool_call_id":format!("call-{i}"),"function_name":"echo","arguments":{}})).collect::<Vec<_>>(),
                    "observation":{"results":(0..width).rev().map(|i| json!({"source_call_id":format!("call-{i}"),"content":"ok"})).collect::<Vec<_>>()}})
            } else { json!({"step_id":1,"source":"user","message":text}) };
            if case == "atif-timestamp" { step["timestamp"] = json!("2026-11-09T12:34:56.123456+05:30"); }
            let mut document = json!({"schema_version":"ATIF-v1.8", "trajectory_id":"bench", "agent":{"name":"bench", "version":"1"}, "steps":[step]});
            if case == "atif-document" { document["steps"] = json!((1..=rows).map(|i| json!({"step_id":i,"source":"user","message":text})).collect::<Vec<_>>()); return document.to_string().into_bytes(); }
            let line = document.to_string();
            format!("{line}\n").repeat(rows).into_bytes()
        }
        "warc" => format!("WARC/1.0\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000001>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: {}\r\n\r\n{text}\r\n\r\n", text.len()).repeat(rows).into_bytes(),
        "parquet" | "parquet-project" => {
            let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![42; rows])), Arc::new(StringArray::from(vec![text.as_str(); rows]))]).unwrap();
            let mut bytes = Vec::new();
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema(), None).unwrap();
            writer.write(&batch).unwrap(); writer.close().unwrap(); bytes
        }
        "service-metadata" => json!({"entries":(0..rows).map(|i| json!({"id":i,"text":text})).collect::<Vec<_>>()}).to_string().into_bytes(),
        "lines" => format!("{text}\n").repeat(rows).into_bytes(),
        _ => vec![b'x'; rows.checked_mul(width.max(1)).unwrap()],
    }
}

type Source =
    Arc<dyn Accessor<Request = ByteReadRequest, Reader = dyn open_arrow_access_core::ByteReader>>;

async fn scan(
    case: &str,
    source: Source,
    rows: usize,
    len: usize,
    batch: usize,
    concurrency: usize,
    io_chunk: usize,
) -> usize {
    let opts = ByteReadOptions {
        io_concurrency: nz(concurrency),
        io_chunk_size: NonZeroUsize::new(io_chunk),
    };
    let bytes = ByteReadRequest::new("data").with_options(opts);
    if case.starts_with("service-") {
        let connection =
            open_arrow_access::service::common::Connection::new("https://benchmark.invalid")
                .unwrap()
                .with_context(opendal::OperationContext::new());
        match case {
            "service-body" => {
                let body = connection
                    .bytes(
                        connection.url("data", &[]).unwrap(),
                        "application/octet-stream",
                    )
                    .await
                    .unwrap();
                assert_eq!(body.len(), len);
                black_box(body);
            }
            "service-metadata" => {
                let document = connection.json("data", &[]).await.unwrap();
                assert_eq!(document.value()["entries"].as_array().unwrap().len(), rows);
                black_box(document);
                return rows;
            }
            _ => {
                let mut reader = connection
                    .open("https://benchmark.invalid/data", opts)
                    .await
                    .unwrap();
                let mut bytes = 0;
                while let Some(chunk) = reader.next().await.unwrap() {
                    bytes += black_box(chunk).len();
                }
                assert_eq!(bytes, len);
            }
        }
        return len;
    }
    if case.starts_with("storage-") {
        let mut reader = source.open(bytes).await.unwrap();
        let mut count = 0;
        match case {
            "storage-sequential" => {
                while let Some(bytes) = reader.next().await.unwrap() {
                    count += black_box(bytes).len();
                }
            }
            "storage-range" => {
                let bytes = reader.read_range(0..len as u64).await.unwrap();
                count = black_box(bytes).len();
            }
            _ => {
                let stride = if case == "storage-adjacent" { 32 } else { 4096 };
                let ranges: Vec<_> = (0..len.saturating_sub(31))
                    .step_by(stride)
                    .take(4096)
                    .map(|n| n as u64..n as u64 + 32)
                    .collect();
                let expected = ranges.len() * 32;
                for b in reader.read_ranges(ranges).await.unwrap() {
                    count += black_box(b).len();
                }
                assert_eq!(count, expected);
                return count;
            }
        }
        assert_eq!(count, len);
        return count;
    }
    if case == "lines" {
        use futures::TryStreamExt;
        let lines =
            open_arrow_access_core::read_lines(source.open(bytes).await.unwrap(), None, "bench");
        futures::pin_mut!(lines);
        let mut count = 0;
        while let Some(line) = lines.try_next().await.unwrap() {
            black_box(line);
            count += 1;
        }
        assert_eq!(count, rows);
        return count;
    }
    if case == "warc" {
        let mut reader = WarcAccessor::new(source).open(bytes).await.unwrap();
        let mut count = 0;
        while let Some(record) = reader.next().await.unwrap() {
            black_box(record);
            count += 1;
        }
        assert_eq!(count, rows);
        return count;
    }
    let options = ReadOptions {
        batch_size: nz(batch),
        projection: (case.ends_with("-project") || case == "atif-tools").then(|| {
            vec![if case.starts_with("atif") {
                "trajectory_id".into()
            } else {
                "id".into()
            }]
        }),
    };
    let request = ReadRequest::new("data")
        .with_options(options)
        .with_byte_options(opts);
    let mut reader = match case {
        "csv" | "csv-project" => CsvAccessor::new(source, schema())
            .open(request)
            .await
            .unwrap(),
        "json" | "json-project" => JsonAccessor::new(source, schema())
            .open(request)
            .await
            .unwrap(),
        "parquet" | "parquet-project" => ParquetAccessor::new(source).open(request).await.unwrap(),
        "atif" | "atif-document" | "atif-project" | "atif-tools" | "atif-timestamp" => {
            AtifAccessor::new(
                source,
                if case == "atif-document" {
                    AtifFormat::Json
                } else {
                    AtifFormat::JsonLines
                },
            )
            .open(request)
            .await
            .unwrap()
        }
        _ => {
            let accessor = XmlAccessor::new(source, "/rows/row");
            if case == "xml-infer" {
                accessor.open(request).await.unwrap()
            } else {
                let prefix = if case == "xml-nested" { "a/b/" } else { "" };
                let schema = if case == "xml-struct" {
                    let mut children = vec![
                        XmlField::new("id", "@id", DataType::Int64, false),
                        XmlField::new("text", "text", DataType::Utf8, false),
                    ];
                    for i in 0..32 {
                        children.push(XmlField::new(
                            format!("optional{i}"),
                            format!("@optional{i}"),
                            DataType::Utf8,
                            true,
                        ));
                    }
                    XmlSchema::new(vec![XmlField::structure(
                        "a",
                        "a",
                        false,
                        vec![XmlField::structure("b", "b", false, children)],
                    )])
                    .unwrap()
                } else {
                    XmlSchema::new(vec![
                        XmlField::new("id", format!("{prefix}id"), DataType::Int64, false),
                        XmlField::new("text", format!("{prefix}text"), DataType::Utf8, false),
                    ])
                    .unwrap()
                };
                accessor.with_schema(schema).open(request).await.unwrap()
            }
        }
    };
    let mut count = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        count += batch.num_rows();
        black_box(batch);
    }
    assert_eq!(count, if case == "atif-document" { 1 } else { rows });
    count
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--list") {
        println!("{}", CASES.join("\n"));
        return;
    }
    if args.first().is_some_and(|arg| arg == "--bench") {
        for case in CASES.iter().filter(|&&case| case != "atif-tools") {
            assert!(
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        *case,
                        if case.starts_with("service-") {
                            "http"
                        } else {
                            "memory"
                        }
                    ])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        return;
    }
    let case = args.first().map(String::as_str).unwrap_or("json");
    assert!(CASES.contains(&case), "unknown case {case}");
    let backend = args.get(1).map(String::as_str).unwrap_or("memory");
    let number = |index: usize, default: usize| {
        args.get(index)
            .map(|v| v.parse::<usize>().unwrap())
            .unwrap_or(default)
    };
    assert!(
        !case.starts_with("service-") || backend == "http",
        "shared service cases require the HTTP replay backend"
    );
    let rows = number(2, 32768);
    let iterations = number(3, 7);
    let width = number(4, 128);
    let chunk = number(5, 65536);
    let batch = number(6, 1024);
    let concurrency = number(7, 1);
    let io_chunk = number(8, 2 * 1024 * 1024);
    assert!(rows > 0 && iterations > 0 && width > 0);
    let bytes = generate(case, rows, width);
    let len = bytes.len();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let http = Transport::new(bytes.clone(), number(9, 16384), number(10, 0));
    opendal::HttpTransporter::install_default(http.clone());
    let source: Source = match backend {
        "memory" => Arc::new(MemoryAccessor::new(&bytes, chunk).with_ranges()),
        "fs" => {
            std::fs::write(dir.path().join("data"), &bytes).unwrap();
            Arc::new(
                OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()))
                    .with_chunk_size(nz(chunk)),
            )
        }
        "http" => {
            let op = opendal::Operator::new(
                opendal_service_http::Http::default().endpoint("https://benchmark.invalid"),
            )
            .unwrap();
            Arc::new(OpenDalAccessor::new(op).with_chunk_size(nz(chunk)))
        }
        "s3" => {
            let op = opendal::Operator::new(
                opendal::services::S3::default()
                    .bucket("benchmark")
                    .region("us-east-1")
                    .endpoint("https://benchmark.invalid")
                    .access_key_id("benchmark")
                    .secret_access_key("benchmark")
                    .disable_config_load()
                    .disable_ec2_metadata(),
            )
            .unwrap();
            Arc::new(OpenDalAccessor::new(op).with_chunk_size(nz(chunk)))
        }
        _ => panic!("backend must be memory, fs, http or s3"),
    };
    drop(bytes);
    // Warm up outside samples. Fixture setup, input generation and runtime startup are excluded.
    runtime.block_on(scan(
        case,
        source.clone(),
        rows,
        len,
        batch,
        concurrency,
        io_chunk,
    ));
    let mut samples = Vec::new();
    for _ in 0..iterations {
        http.reset();
        let start = Instant::now();
        let units = runtime.block_on(scan(
            case,
            source.clone(),
            rows,
            len,
            batch,
            concurrency,
            io_chunk,
        ));
        let elapsed = start.elapsed().as_secs_f64();
        samples
            .push(json!({"seconds":elapsed, "units":units, "http_requests":http.requests(), "http_response_bytes":http.bytes()}));
    }
    println!(
        "{}",
        json!({"case":case,"backend":backend,"rows":rows,"input_bytes":len,"width":width,"chunk":chunk,"batch":batch,"concurrency":concurrency,"io_chunk":io_chunk,"http_chunk":number(9,16384),"latency_ms":number(10,0),"samples":samples})
    );
}
