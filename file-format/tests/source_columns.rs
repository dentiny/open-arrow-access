#![cfg(feature = "warc")]
use arrow_array::{StringArray, StringViewArray, UInt64Array};
use open_arrow_access_core::{
    Accessor, AccessorExt, ReadOptions, ReadRequest, RecordReader, ScanPlanner,
};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_decompression::AutoDecompressionLayer;
use open_arrow_access_file_format::{DecodePartitionOptions, warc::WarcArrowAccessor};
use open_arrow_access_layers::{
    ColumnEvolution, ParallelReadLayer, SchemaEvolutionLayer, SourceColumnsLayer,
};
use open_arrow_access_test_support::MemoryAccessor;
use std::{
    io::{Read, Write},
    num::NonZeroUsize,
};

fn records() -> Vec<Vec<u8>> {
    (0..3).map(|i| format!("WARC/1.1\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:test:{i}>\r\nWARC-Date: 2020-07-08T02:52:55Z\r\nContent-Length: 5\r\n\r\nbody{i}\r\n\r\n").into_bytes()).collect()
}
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}
type Row = (String, u64, u64);
async fn rows(
    mut reader: Box<dyn RecordReader>,
    offset: &str,
    decoded: &str,
    object: &str,
) -> Vec<Row> {
    let mut result = Vec::new();
    let schema = reader.metadata().schema.clone();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        let offsets = batch
            .column_by_name(offset)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let decoded = batch
            .column_by_name(decoded)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let objects = batch
            .column_by_name(object)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            result.push((
                objects.value(i).to_owned(),
                offsets.value(i),
                decoded.value(i),
            ));
        }
    }
    result
}
fn columns() -> SourceColumnsLayer {
    SourceColumnsLayer::new()
        .with_object("path")
        .with_offset("offset")
        .with_offset_in_frame("decoded")
}
#[tokio::test]
async fn record_coordinates_survive_projection_parallelism_and_resume() {
    let records = records();
    for mode in 0..3 {
        let members = records.iter().map(|r| gzip(r)).collect::<Vec<_>>();
        let input = match mode {
            0 => records.concat(),
            1 => members.concat(),
            _ => gzip(&records.concat()),
        };
        let object = if mode == 0 {
            "data.warc"
        } else {
            "data.warc.gz"
        };
        let mut stored = 0;
        let mut decoded = 0;
        let expected = records
            .iter()
            .enumerate()
            .map(|(i, record)| {
                let result = (
                    object.to_owned(),
                    if mode == 2 { 0 } else { stored },
                    if mode == 2 { decoded } else { 0 },
                );
                stored += if mode == 1 {
                    members[i].len() as u64
                } else {
                    record.len() as u64
                };
                decoded += record.len() as u64;
                result
            })
            .collect::<Vec<_>>();
        for chunk in [1, input.len()] {
            let columns = columns();
            let source = WarcArrowAccessor::new(
                MemoryAccessor::new(&input, chunk).layer(columns.byte_positions()),
            )
            .with_partition_options(DecodePartitionOptions {
                target_bytes: NonZeroUsize::MIN,
            })
            .layer(columns);
            let request = ReadRequest::new(object).with_options(ReadOptions {
                batch_size: NonZeroUsize::MIN,
                projection: Some(vec![
                    "record_id".into(),
                    "offset".into(),
                    "decoded".into(),
                    "path".into(),
                ]),
            });
            assert_eq!(
                rows(
                    source.open(request.clone()).await.unwrap(),
                    "offset",
                    "decoded",
                    "path"
                )
                .await,
                expected
            );
            let source = source.layer(ParallelReadLayer::new(
                NonZeroUsize::new(3).unwrap(),
                NonZeroUsize::MIN,
            ));
            assert_eq!(
                rows(
                    source.open(request.clone()).await.unwrap(),
                    "offset",
                    "decoded",
                    "path"
                )
                .await,
                expected
            );
            assert_eq!(
                rows(
                    Box::new(source.plan_scan(request).await.unwrap().into_reader()),
                    "offset",
                    "decoded",
                    "path"
                )
                .await,
                expected
            );
        }
        for (i, (_, offset, decoded)) in expected.iter().enumerate() {
            let tail = &input[*offset as usize..];
            let mut resumed = Vec::new();
            if mode == 0 {
                resumed.extend_from_slice(tail);
            } else {
                flate2::read::MultiGzDecoder::new(tail)
                    .read_to_end(&mut resumed)
                    .unwrap();
            }
            assert!(resumed[*decoded as usize..].starts_with(&records[i]));
        }
    }
}
#[tokio::test]
async fn file_set_union_parallel_and_generated_columns_compose() {
    let input = gzip(&records().concat());
    for union in [false, true] {
        let format = WarcArrowAccessor::new(
            MemoryAccessor::new(&input, 7).layer(columns().byte_positions()),
        )
        .with_partition_options(DecodePartitionOptions {
            target_bytes: NonZeroUsize::MIN,
        });
        let files = FileSetAccessor::new(format).with_file_planning();
        let files = if union {
            files.with_union_by_name()
        } else {
            files
        };
        let source = files.layer(columns()).layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ));
        let request = FileSetRequest::new(["first.warc.gz", "second.warc.gz"]);
        let result = rows(
            source.open(request.clone()).await.unwrap(),
            "offset",
            "decoded",
            "path",
        )
        .await;
        assert_eq!(result.len(), 6);
        assert_eq!(
            (result[0].0.as_str(), result[3].0.as_str()),
            ("first.warc.gz", "second.warc.gz")
        );
        assert_eq!(result[1].2, records()[0].len() as u64);
        let source = source.layer(
            SchemaEvolutionLayer::try_new(vec![
                ColumnEvolution::add_column_sql(
                    "docid",
                    "concat(path, ':', cast(decoded as varchar))",
                ),
                ColumnEvolution::remove_column("decoded"),
            ])
            .unwrap(),
        );
        let mut reader = source.open(request).await.unwrap();
        assert!(reader.metadata().schema.index_of("decoded").is_err());
        let batch = reader.next().await.unwrap().unwrap();
        let ids = batch.column_by_name("docid").unwrap();
        let id = if let Some(ids) = ids.as_any().downcast_ref::<StringArray>() {
            ids.value(0)
        } else {
            ids.as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(0)
        };
        assert_eq!(id, "first.warc.gz:0");
    }
}
#[tokio::test]
async fn existing_decompression_is_unchanged_and_cannot_silently_supply_stored_offsets() {
    use open_arrow_access_core::ErrorKind;
    let input = gzip(&records().concat());
    let source =
        WarcArrowAccessor::new(MemoryAccessor::new(&input, 3).layer(AutoDecompressionLayer::new()))
            .layer(columns());
    assert_eq!(
        source
            .open(ReadRequest::new("data.warc.gz"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    let source = MemoryAccessor::new(&input, 3)
        .layer(AutoDecompressionLayer::new())
        .layer(columns().byte_positions());
    assert_eq!(
        source
            .open("data.warc.gz".into())
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
}
#[cfg(feature = "csv")]
#[tokio::test]
async fn object_is_generic_but_offsets_fail_before_storage_open() {
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_file_format::csv::CsvAccessor;
    let input = MemoryAccessor::new(b"id\n1\n2\n", 1);
    let observations = input.observations.clone();
    let source = CsvAccessor::infer(input.clone()).layer(columns());
    assert_eq!(
        source
            .open(ReadRequest::new("data.csv"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        source
            .plan_scan(ReadRequest::new("data.csv"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert!(observations.requests.lock().unwrap().is_empty());
    let source = CsvAccessor::infer(input).layer(SourceColumnsLayer::new().with_object("path"));
    let mut reader = source
        .open(ReadRequest::new("data.csv").with_options(ReadOptions {
            projection: Some(vec!["path".into()]),
            ..Default::default()
        }))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "data.csv"
    );
}
#[tokio::test]
async fn collisions_fail_and_empty_streams_keep_their_schema() {
    use open_arrow_access_core::ErrorKind;
    let source = WarcArrowAccessor::new(MemoryAccessor::new(&records().concat(), 3))
        .layer(SourceColumnsLayer::new().with_offset("record_id"));
    assert_eq!(
        source
            .open(ReadRequest::new("data.warc"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
    let source = WarcArrowAccessor::new(
        MemoryAccessor::new(&gzip(b""), 1).layer(columns().byte_positions()),
    )
    .layer(columns());
    let mut reader = source
        .open(ReadRequest::new("empty.warc.gz"))
        .await
        .unwrap();
    assert!(reader.metadata().schema.index_of("offset").is_ok());
    assert!(reader.next().await.unwrap().is_none());
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn parquet_rejects_offsets_before_opening_bytes() {
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    let input = MemoryAccessor::new(b"not parquet", 1).with_ranges();
    let observations = input.observations.clone();
    let source = ParquetAccessor::new(input).layer(columns());
    assert_eq!(
        source
            .open(ReadRequest::new("data.parquet"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        source
            .plan_scan(ReadRequest::new("data.parquet"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert!(observations.requests.lock().unwrap().is_empty());
}
