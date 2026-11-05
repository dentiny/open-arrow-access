//! Run fixture generation and reads in separate processes so peak RSS excludes writing.
//! See docs/performance.md for commands and the measured baseline.

use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Instant};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ByteReadOptions, ReadOptions, ReadRequest, file_format::ParquetAccessor,
    storage::FsAccessor,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

const COLUMNS: usize = 8;

fn generate(path: &Path, rows: usize) {
    let schema = Arc::new(Schema::new(
        (0..COLUMNS)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let suffix = "x".repeat(120);
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("{row:08}{suffix}")),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![values; COLUMNS]).unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(rows))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        schema,
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_eq!(metadata.num_row_groups(), 1);
    assert_eq!(metadata.file_metadata().num_rows(), rows as i64);
}

#[tokio::main(worker_threads = 2)]
async fn main() {
    let args = std::env::args()
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    if args.len() < 4 {
        eprintln!("generate ABSOLUTE_PATH ROWS | read ABSOLUTE_PATH narrow|all [CONCURRENCY]");
        return;
    }
    let path = Path::new(&args[2]);
    if args[1] == "generate" {
        generate(path, args[3].parse().unwrap());
        return;
    }
    assert_eq!(args[1], "read");
    let projection = match args[3].as_str() {
        "narrow" => Some(vec!["c0".into()]),
        "all" => None,
        _ => panic!("projection must be narrow or all"),
    };
    let concurrency = args.get(4).map_or(1, |s| s.parse().unwrap());
    let start = Instant::now();
    let accessor = ParquetAccessor::new(FsAccessor::new(path.parent().unwrap()).await.unwrap());
    let mut reader = accessor
        .open(
            ReadRequest::new(path.file_name().unwrap().to_str().unwrap())
                .with_options(ReadOptions {
                    projection,
                    ..ReadOptions::default()
                })
                .with_byte_options(ByteReadOptions {
                    io_concurrency: NonZeroUsize::new(concurrency).unwrap(),
                    ..ByteReadOptions::default()
                }),
        )
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
        std::hint::black_box(batch);
    }
    println!(
        "rows={rows} elapsed_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
}
