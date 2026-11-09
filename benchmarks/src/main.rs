//! One workload per process; fixture preparation is outside the timed interval.
mod source;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ReadOptions, ReadRequest, RecordReader, read_all,
    read_lines,
};
use open_arrow_access_file_format::{
    atif::{AtifAccessor, AtifFormat},
    csv::CsvAccessor,
    json::JsonAccessor,
    parquet::ParquetAccessor,
    warc::WarcAccessor,
    xml::{XmlAccessor, XmlField, XmlSchema},
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde_json::json;
use std::{
    env,
    fs::{self, File},
    hint::black_box,
    io::{BufWriter, Write},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::Instant,
};

type AnyResult<T> = Result<T, Box<dyn std::error::Error>>;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

fn payload(id: usize, width: usize) -> String {
    let mut state = (id as u64).wrapping_add(0x9e3779b97f4a7c15);
    (0..width)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (b'a' + (state % 26) as u8) as char
        })
        .collect()
}

fn generate(root: &Path, rows: usize, width: usize) -> AnyResult<()> {
    fs::create_dir_all(root)?;
    if rows == 0 || width == 0 {
        return Err("rows and width must be positive".into());
    }
    let mut csv = BufWriter::new(File::create(root.join("data.csv"))?);
    let mut jsonl = BufWriter::new(File::create(root.join("data.jsonl"))?);
    let mut xml = BufWriter::new(File::create(root.join("data.xml"))?);
    let mut atif = BufWriter::new(File::create(root.join("data.atif"))?);
    let mut warc = BufWriter::new(File::create(root.join("data.warc"))?);
    let mut lines = BufWriter::new(File::create(root.join("data.lines"))?);
    let mut parquet = ArrowWriter::try_new(
        File::create(root.join("data.parquet"))?,
        schema(),
        Some(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_row_count(Some(8192))
                .build(),
        ),
    )?;
    write!(xml, "<rows>")?;
    for id in 0..rows {
        let text = payload(id, width);
        writeln!(csv, "{id},{text}")?;
        writeln!(jsonl, "{}", json!({"id":id,"text":text}))?;
        write!(xml, "<row><id>{id}</id><text>{text}</text></row>")?;
        writeln!(
            atif,
            "{}",
            json!({"schema_version":"ATIF-v1.8","trajectory_id":id.to_string(),"agent":{"name":"bench","version":"1"},"steps":[{"step_id":1,"source":"user","message":text}]})
        )?;
        write!(
            warc,
            "WARC/1.1\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:bench:{id}>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: {}\r\n\r\n{text}\r\n\r\n",
            text.len()
        )?;
        writeln!(lines, "{text}")?;
    }
    for start in (0..rows).step_by(8192) {
        let end = (start + 8192).min(rows);
        parquet.write(&RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(start as i64..end as i64)),
                Arc::new(StringArray::from_iter_values(
                    (start..end).map(|id| payload(id, width)),
                )),
            ],
        )?)?;
    }
    parquet.close()?;
    write!(xml, "</rows>")?;
    fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&json!({"rows":rows,"width":width,"generator_version":1}))?,
    )?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> AnyResult<()> {
    let args: Vec<_> = env::args().collect();
    if args.get(1).map(String::as_str) == Some("generate") {
        if args.len() != 5 {
            return Err("usage: generate ROOT ROWS WIDTH".into());
        }
        return generate(Path::new(&args[2]), args[3].parse()?, args[4].parse()?);
    }
    if args.len() != 10 {
        return Err("usage: oaa-bench ROOT BACKEND CASE CHUNK BATCH CONCURRENCY IO_CHUNK PROJECTION ITERATIONS; or generate ROOT ROWS WIDTH".into());
    }
    let root = Path::new(&args[1]);
    let backend = &args[2];
    let case = args[3].as_str();
    let chunk = NonZeroUsize::new(args[4].parse()?).ok_or("zero chunk")?;
    let batch_size = NonZeroUsize::new(args[5].parse()?).ok_or("zero batch")?;
    let byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(args[6].parse()?).ok_or("zero concurrency")?,
        io_chunk_size: Some(NonZeroUsize::new(args[7].parse()?).ok_or("zero I/O chunk")?),
    };
    let projection = match args[8].as_str() {
        "all" => None,
        "empty" => Some(vec![]),
        other => Some(other.split(',').map(str::to_owned).collect()),
    };
    let options = ReadOptions {
        batch_size,
        projection,
    };
    let iterations: usize = args[9].parse()?;
    if iterations == 0 {
        return Err("zero iterations".into());
    }
    let extension = match case {
        "csv" => "csv",
        "json" => "jsonl",
        "xml" | "xml-infer" => "xml",
        "atif" => "atif",
        "warc" => "warc",
        "parquet" => "parquet",
        "lines" | "all" | "sequential" | "ranges" => "lines",
        _ => return Err("unknown case".into()),
    };
    let name = format!("data.{extension}");
    let input_bytes = fs::metadata(root.join(&name))?.len();
    let source = source::Source::new(root, backend, &name, chunk).await?;
    let started = Instant::now();
    let mut first_item_ns = None;
    let mut rows = 0_u64;
    let mut output_bytes = 0_u64;
    for _ in 0..iterations {
        let request = ReadRequest::new(name.clone())
            .with_options(options.clone())
            .with_byte_options(byte_options);
        let bytes = ByteReadRequest::new(name.clone()).with_options(byte_options);
        let mut record_reader: Option<Box<dyn RecordReader>> = match case {
            "csv" => Some(
                CsvAccessor::new(source.clone(), schema())
                    .open(request)
                    .await?,
            ),
            "json" => Some(
                JsonAccessor::new(source.clone(), schema())
                    .open(request)
                    .await?,
            ),
            "parquet" => Some(ParquetAccessor::new(source.clone()).open(request).await?),
            "atif" => Some(
                AtifAccessor::new(source.clone(), AtifFormat::JsonLines)
                    .open(request)
                    .await?,
            ),
            "xml" => Some(
                XmlAccessor::new(source.clone(), "/rows/row")
                    .with_schema(XmlSchema::new(vec![
                        XmlField::new("id", "id", DataType::Int64, false),
                        XmlField::new("text", "text", DataType::Utf8, false),
                    ])?)
                    .open(request)
                    .await?,
            ),
            "xml-infer" => Some(
                XmlAccessor::new(source.clone(), "/rows/row")
                    .open(request)
                    .await?,
            ),
            _ => None,
        };
        if let Some(reader) = record_reader.as_mut() {
            while let Some(batch) = reader.next().await? {
                first_item_ns.get_or_insert_with(|| started.elapsed().as_nanos());
                rows += batch.num_rows() as u64;
                output_bytes += batch.get_array_memory_size() as u64;
                black_box(batch);
            }
        } else if case == "warc" {
            let mut reader = WarcAccessor::new(source.clone()).open(bytes).await?;
            while let Some(record) = reader.next().await? {
                first_item_ns.get_or_insert_with(|| started.elapsed().as_nanos());
                rows += 1;
                black_box(record);
            }
        } else {
            let mut reader = source.open(bytes).await?;
            match case {
                "lines" => {
                    let lines = read_lines(reader, None, "bench");
                    futures::pin_mut!(lines);
                    while let Some(line) = lines.try_next().await? {
                        first_item_ns.get_or_insert_with(|| started.elapsed().as_nanos());
                        rows += 1;
                        output_bytes += line.len() as u64;
                        black_box(line);
                    }
                }
                "all" => {
                    let data = read_all(reader, "bench").await?;
                    output_bytes += data.len() as u64;
                    black_box(data);
                }
                "ranges" => {
                    let ranges = (0..input_bytes)
                        .step_by(8192)
                        .map(|offset| offset..(offset + 4096).min(input_bytes))
                        .collect();
                    for data in reader.read_ranges(ranges).await? {
                        output_bytes += data.len() as u64;
                        black_box(data);
                    }
                }
                _ => {
                    while let Some(data) = reader.next().await? {
                        output_bytes += data.len() as u64;
                        black_box(data);
                    }
                }
            }
        }
    }
    let elapsed_ns = started.elapsed().as_nanos();
    println!(
        "{}",
        json!({"compiler":env!("OAA_BENCH_COMPILER"),"backend":backend,"case":case,"chunk":chunk.get(),"batch":batch_size.get(),"concurrency":byte_options.io_concurrency.get(),"io_chunk":byte_options.io_chunk_size.unwrap().get(),"projection":args[8],"iterations":iterations,"input_bytes":input_bytes,"elapsed_ns":elapsed_ns,"first_item_ns":first_item_ns,"rows":rows,"output_bytes":output_bytes,"logical_io":source.stats()})
    );
    Ok(())
}
