//! Deterministic local read matrix. Run with --release --features directory.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Accessor, AccessorExt, ReadRequest, RecordReader, ScanPlanner};
use open_arrow_access_dataset::{
    directory::{DirectoryAccessor, DirectoryRequest},
    file_set::{FileSetAccessor, FileSetRequest},
};
use open_arrow_access_file_format::{
    atif::{AtifAccessor, AtifFormat},
    csv::CsvAccessor,
    json::JsonAccessor,
    parquet::ParquetAccessor,
    warc::WarcArrowAccessor,
    xml::XmlAccessor,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_storage::OpenDalAccessor;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{io::Write, num::NonZeroUsize, sync::Arc, time::Instant};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
async fn scan<A: Accessor<Reader = dyn RecordReader>>(
    a: &A,
    r: A::Request,
    expected: usize,
) -> Result<(f64, f64, usize)> {
    let start = Instant::now();
    let mut reader = a.open(r).await?;
    let mut rows = 0;
    let mut batches = 0;
    let mut first = 0.;
    while let Some(batch) = reader.next().await? {
        if batches == 0 {
            first = start.elapsed().as_secs_f64();
        }
        rows += batch.num_rows();
        batches += 1;
        std::hint::black_box(batch);
    }
    assert_eq!(rows, expected);
    Ok((start.elapsed().as_secs_f64(), first, batches))
}
async fn matrix<A>(
    format: &str,
    a: A,
    storage: OpenDalAccessor,
    shards: usize,
    rows: usize,
    reps: usize,
) -> Result<()>
where
    A: ScanPlanner<Request = ReadRequest> + Clone + 'static,
{
    for dataset in ["single", "files", "directory"] {
        for native in [false, true] {
            if dataset == "single" && !native {
                continue;
            }
            for workers in [0, 1, 2, 4, 8] {
                if workers == 0 && !native {
                    continue;
                }
                for rep in 0..=reps {
                    let expected = if dataset == "single" {
                        rows
                    } else {
                        rows * shards
                    };
                    let layer = ParallelReadLayer::new(nz(workers.max(1)), nz(2));
                    let result = match dataset {
                        "single" => {
                            let r = ReadRequest::new(format!("{format}/000.{format}"));
                            if workers == 0 {
                                scan(&a, r, expected).await?
                            } else {
                                scan(&a.clone().layer(layer), r, expected).await?
                            }
                        }
                        "files" => {
                            let mut files = FileSetAccessor::new(a.clone());
                            if native {
                                files = files.with_file_planning();
                            }
                            let r = FileSetRequest::new(
                                (0..shards).map(|i| format!("{format}/{i:03}.{format}")),
                            );
                            if workers == 0 {
                                scan(&files, r, expected).await?
                            } else {
                                scan(&files.layer(layer), r, expected).await?
                            }
                        }
                        _ => {
                            let mut dir = DirectoryAccessor::new(storage.clone(), a.clone());
                            if native {
                                dir = dir.with_file_planning();
                            }
                            let r = DirectoryRequest::new(format!("{format}/"));
                            if workers == 0 {
                                scan(&dir, r, expected).await?
                            } else {
                                scan(&dir.layer(layer), r, expected).await?
                            }
                        }
                    };
                    if rep > 0 {
                        println!(
                            "{format},{dataset},{native},{workers},{rep},{expected},{},{:.9},{:.9}",
                            result.2, result.0, result.1
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let rows = std::env::var("BENCH_ROWS")
        .unwrap_or("32768".into())
        .parse::<usize>()?;
    let shards = std::env::var("BENCH_SHARDS")
        .unwrap_or("8".into())
        .parse::<usize>()?;
    let reps = std::env::var("BENCH_REPS")
        .unwrap_or("5".into())
        .parse::<usize>()?;
    assert!(
        rows > 0 && shards > 0 && reps > 0,
        "benchmark dimensions must be positive"
    );
    let filter = std::env::var("BENCH_FORMAT").unwrap_or_default();
    let root = tempfile::tempdir()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let trajectory = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../testdata/atif/trajectories.jsonl"
    ))?
    .lines()
    .find(|s| !s.trim().is_empty())
    .unwrap()
    .to_owned();
    println!("format,dataset,native,workers,rep,rows,batches,seconds,first_batch_seconds");
    for format in ["csv", "json", "parquet", "xml", "atif", "warc"] {
        if !filter.is_empty() && !filter.split(',').any(|f| f == format) {
            continue;
        }
        let dir = root.path().join(format);
        std::fs::create_dir(&dir)?;
        let mut data = Vec::new();
        if format == "csv" {
            writeln!(data, "id,text")?;
        }
        if format == "xml" {
            write!(data, "<rows>")?;
        }
        for i in 0..rows {
            match format {
                "csv" => writeln!(data, "{i},text-{i:08}-abcdefghijklmnopqrstuvwxyz")?,
                "json" => writeln!(
                    data,
                    "{{\"id\":{i},\"text\":\"text-{i:08}-abcdefghijklmnopqrstuvwxyz\"}}"
                )?,
                "xml" => write!(
                    data,
                    "<row><id>{i}</id><text>text-{i:08}-abcdefghijklmnopqrstuvwxyz</text></row>"
                )?,
                "atif" => writeln!(data, "{trajectory}")?,
                "warc" => write!(
                    data,
                    "WARC/1.0\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: 32\r\n\r\nabcdefghijklmnopqrstuvwxyz012345\r\n\r\n"
                )?,
                _ => {}
            }
        }
        if format == "xml" {
            write!(data, "</rows>")?;
        }
        if format == "parquet" {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i as i64))),
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|i| format!("text-{i:08}-abcdefghijklmnopqrstuvwxyz")),
                    )),
                ],
            )?;
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(8192))
                .build();
            let mut writer = ArrowWriter::try_new(&mut data, schema.clone(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }
        for i in 0..shards {
            std::fs::write(dir.join(format!("{i:03}.{format}")), &data)?;
        }
        eprintln!(
            "{format}: {rows} rows/shard, {} bytes/shard, {shards} shards",
            data.len()
        );
        let storage =
            OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(root.path()));
        match format {
            "csv" => {
                matrix(
                    format,
                    CsvAccessor::new(storage.clone(), schema.clone()).with_dialect(
                        open_arrow_access_file_format::csv::CsvDialect {
                            has_header: true,
                            ..Default::default()
                        },
                    ),
                    storage,
                    shards,
                    rows,
                    reps,
                )
                .await?
            }
            "json" => {
                matrix(
                    format,
                    JsonAccessor::new(storage.clone(), schema.clone()),
                    storage,
                    shards,
                    rows,
                    reps,
                )
                .await?
            }
            "parquet" => {
                matrix(
                    format,
                    ParquetAccessor::new(storage.clone()),
                    storage,
                    shards,
                    rows,
                    reps,
                )
                .await?
            }
            "xml" => {
                matrix(
                    format,
                    XmlAccessor::new(storage.clone(), "/rows/row"),
                    storage,
                    shards,
                    rows,
                    reps,
                )
                .await?
            }
            "atif" => {
                matrix(
                    format,
                    AtifAccessor::new(storage.clone(), AtifFormat::JsonLines),
                    storage,
                    shards,
                    rows,
                    reps,
                )
                .await?
            }
            "warc" => {
                matrix(
                    format,
                    WarcArrowAccessor::new(storage.clone()),
                    storage,
                    shards,
                    rows,
                    reps,
                )
                .await?
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
