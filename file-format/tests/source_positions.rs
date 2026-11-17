#![cfg(feature = "warc")]
use arrow_array::{StringArray, StringViewArray, UInt64Array};
use open_arrow_access_core::{
    Accessor, AccessorExt, ReadOptions, ReadRequest, RecordReader, ScanPlanner, SourceColumn,
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
#[tokio::test]
async fn warc_positions_are_resumable_and_identical_in_parallel_planning() {
    let records = records();
    let plain = records.concat();
    for mode in 0..3 {
        let members = records.iter().map(|r| gzip(r)).collect::<Vec<_>>();
        let input = match mode {
            0 => plain.clone(),
            1 => members.concat(),
            _ => gzip(&plain),
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
                let row = (
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
                row
            })
            .collect::<Vec<_>>();
        for chunk in [1, input.len()] {
            let format = WarcArrowAccessor::new(
                MemoryAccessor::new(&input, chunk).layer(AutoDecompressionLayer::new()),
            )
            .with_partition_options(DecodePartitionOptions {
                target_bytes: NonZeroUsize::MIN,
            });
            let request = ReadRequest::new(object).with_options(ReadOptions {
                batch_size: NonZeroUsize::MIN,
                projection: Some(vec!["record_id".into()]),
                source_columns: vec![
                    SourceColumn::OffsetInFrame,
                    SourceColumn::Object,
                    SourceColumn::Offset,
                ],
            });
            let reader = format.open(request.clone()).await.unwrap();
            assert_eq!(
                reader
                    .metadata()
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                [
                    "record_id",
                    "_source_offset_in_frame",
                    "_source_object",
                    "_source_offset"
                ]
            );
            assert_eq!(
                rows(
                    reader,
                    "_source_offset",
                    "_source_offset_in_frame",
                    "_source_object"
                )
                .await,
                expected
            );
            let parallel = format.layer(ParallelReadLayer::new(
                NonZeroUsize::new(3).unwrap(),
                NonZeroUsize::MIN,
            ));
            let plan = parallel.plan_scan(request.clone()).await.unwrap();
            assert_eq!(
                rows(
                    Box::new(plan.into_reader()),
                    "_source_offset",
                    "_source_offset_in_frame",
                    "_source_object"
                )
                .await,
                expected
            );
            assert_eq!(
                rows(
                    parallel.open(request).await.unwrap(),
                    "_source_offset",
                    "_source_offset_in_frame",
                    "_source_object"
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
async fn file_set_union_parallel_rename_and_generated_columns_compose() {
    let input = records().concat();
    for union in [false, true] {
        let format = WarcArrowAccessor::new(MemoryAccessor::new(&input, 7)).with_partition_options(
            DecodePartitionOptions {
                target_bytes: NonZeroUsize::MIN,
            },
        );
        let files = FileSetAccessor::new(format).with_file_planning();
        let files = if union {
            files.with_union_by_name()
        } else {
            files
        };
        let source = files
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ))
            .layer(
                SourceColumnsLayer::new()
                    .with_object("path")
                    .with_offset("offset")
                    .with_offset_in_frame("decoded"),
            );
        let request = FileSetRequest::new(["first.warc", "second.warc"]);
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
            ("first.warc", "second.warc")
        );
        assert_eq!(result[0].1, 0);
        assert_eq!(result[1].1, records()[0].len() as u64);
        assert_eq!(result[3].1, 0);
        let result = rows(
            Box::new(
                source
                    .plan_scan(request.clone())
                    .await
                    .unwrap()
                    .into_reader(),
            ),
            "offset",
            "decoded",
            "path",
        )
        .await;
        assert_eq!(result.len(), 6);
        let source = source.layer(
            SchemaEvolutionLayer::try_new(vec![
                ColumnEvolution::add_column_sql(
                    "docid",
                    "concat(path, ':', cast(offset as varchar))",
                ),
                ColumnEvolution::remove_column("offset"),
            ])
            .unwrap(),
        );
        let mut reader = source.open(request).await.unwrap();
        assert!(reader.metadata().schema.index_of("offset").is_err());
        let first = reader.next().await.unwrap().unwrap();
        let ids = first.column_by_name("docid").unwrap();
        let value = if let Some(ids) = ids.as_any().downcast_ref::<StringArray>() {
            ids.value(0)
        } else {
            ids.as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(0)
        };
        assert_eq!(value, "first.warc:0");
    }
}
#[cfg(feature = "csv")]
#[tokio::test]
async fn unsupported_offsets_fail_before_storage_open_and_object_collisions_fail() {
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_file_format::csv::CsvAccessor;
    use std::sync::{Arc, atomic::Ordering};
    let input = MemoryAccessor::new(b"id\n1\n", 1);
    let observations = input.observations.clone();
    let source = CsvAccessor::infer(input).layer(SourceColumnsLayer::new().with_offset("offset"));
    assert_eq!(
        source
            .open(ReadRequest::new("data.csv"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert!(observations.requests.lock().unwrap().is_empty());
    assert_eq!(observations.reads.load(Ordering::SeqCst), 0);
    let input = MemoryAccessor::new(b"_source_object\n1\n", 1);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "_source_object",
        DataType::Int64,
        false,
    )]));
    let source =
        CsvAccessor::new(input, schema).layer(SourceColumnsLayer::new().with_object("path"));
    assert_eq!(
        source
            .open(ReadRequest::new("data.csv"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
}
#[tokio::test]
async fn rename_collisions_and_projection_are_checked_at_open() {
    let source = WarcArrowAccessor::new(MemoryAccessor::new(&records().concat(), 3))
        .layer(SourceColumnsLayer::new().with_offset("record_id"));
    assert_eq!(
        source
            .open(ReadRequest::new("data.warc"))
            .await
            .err()
            .unwrap()
            .kind(),
        open_arrow_access_core::ErrorKind::InvalidInput
    );
    let source = WarcArrowAccessor::new(MemoryAccessor::new(&records().concat(), 3))
        .layer(SourceColumnsLayer::new().with_offset("offset"));
    let request = ReadRequest::new("data.warc").with_options(ReadOptions {
        projection: Some(vec!["offset".into()]),
        ..Default::default()
    });
    let mut reader = source.open(request).await.unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 1);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 3);
}

struct UnknownPosition(Box<dyn open_arrow_access_core::ByteReader>);
#[open_arrow_access_core::async_trait]
impl open_arrow_access_core::Reader for UnknownPosition {
    type Item = open_arrow_access_core::Bytes;
    type Metadata = open_arrow_access_core::ByteMetadata;
    type Capability = open_arrow_access_core::Capability;
    fn capability(&self) -> Self::Capability {
        self.0.capability()
    }
    fn metadata(&self) -> &Self::Metadata {
        self.0.metadata()
    }
    async fn next(&mut self) -> open_arrow_access_core::Result<Option<Self::Item>> {
        self.0.next().await
    }
}
impl open_arrow_access_core::ByteReader for UnknownPosition {}
#[tokio::test]
async fn unknown_byte_coordinates_fail_instead_of_fabricating_positions() {
    use open_arrow_access_core::{ByteReadOptions, ByteReadRequest, ErrorKind, ReaderFactoryExt};
    use open_arrow_access_decompression::{DecompressionFactory, gzip::Gzip};
    use open_arrow_access_file_format::warc::WarcArrowFormat;
    for compressed in [false, true] {
        let bytes = if compressed {
            gzip(&records().concat())
        } else {
            records().concat()
        };
        let input = MemoryAccessor::new(&bytes, 7)
            .open("unknown.warc".into())
            .await
            .unwrap();
        let input: Box<dyn open_arrow_access_core::ByteReader> = Box::new(UnknownPosition(input));
        let input = if compressed {
            DecompressionFactory::new(Gzip)
                .open_input(
                    input,
                    ByteReadRequest::new("unknown.warc").with_options(ByteReadOptions {
                        track_positions: true,
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
        } else {
            input
        };
        assert_eq!(input.position(), None);
        let error = WarcArrowFormat
            .open_input(
                input,
                ReadRequest::new("unknown.warc").with_options(ReadOptions {
                    source_columns: vec![SourceColumn::Offset],
                    ..Default::default()
                }),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(error.to_string().contains("unknown.warc"));
    }
}
#[cfg(feature = "parquet")]
#[tokio::test]
async fn parquet_offsets_fail_before_open_in_sequential_and_scan_paths() {
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    let input = MemoryAccessor::new(b"not parquet", 1).with_ranges();
    let observations = input.observations.clone();
    let source = ParquetAccessor::new(input).layer(SourceColumnsLayer::new().with_offset("offset"));
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

#[cfg(feature = "csv")]
#[tokio::test]
async fn object_column_is_available_for_csv_and_file_planning_with_empty_projection() {
    use open_arrow_access_file_format::csv::CsvAccessor;
    let input = MemoryAccessor::new(b"id\n1\n2\n", 1);
    let source = CsvAccessor::infer(input);
    let request = ReadRequest::new("data.csv").with_options(ReadOptions {
        source_columns: vec![SourceColumn::Object],
        projection: Some(vec![]),
        ..Default::default()
    });
    for mut reader in [
        source.open(request.clone()).await.unwrap(),
        Box::new(source.plan_scan(request).await.unwrap().into_reader()) as Box<dyn RecordReader>,
    ] {
        assert_eq!(reader.metadata().schema.fields().len(), 1);
        let mut count = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            let objects = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for object in objects.iter() {
                assert_eq!(object, Some("data.csv"));
            }
            count += batch.num_rows();
        }
        assert_eq!(count, 2);
    }
}
#[tokio::test]
async fn direct_file_set_projection_preserves_requested_source_columns_in_a_union() {
    let source = FileSetAccessor::new(WarcArrowAccessor::new(MemoryAccessor::new(
        &records().concat(),
        7,
    )))
    .with_union_by_name()
    .with_file_planning();
    let request = FileSetRequest::new(["first.warc", "second.warc"]).with_options(ReadOptions {
        source_columns: vec![
            SourceColumn::OffsetInFrame,
            SourceColumn::Object,
            SourceColumn::Offset,
        ],
        projection: Some(vec!["record_id".into()]),
        ..Default::default()
    });
    let result = rows(
        source.open(request).await.unwrap(),
        "_source_offset",
        "_source_offset_in_frame",
        "_source_object",
    )
    .await;
    assert_eq!(result.len(), 6);
}
#[tokio::test]
async fn compressed_empty_warc_has_a_fixed_source_schema_and_no_rows() {
    let source = WarcArrowAccessor::new(
        MemoryAccessor::new(&gzip(b""), 1).layer(AutoDecompressionLayer::new()),
    )
    .layer(SourceColumnsLayer::new().with_offset("offset"));
    let mut reader = source
        .open(ReadRequest::new("empty.warc.gz"))
        .await
        .unwrap();
    assert!(reader.metadata().schema.index_of("offset").is_ok());
    assert!(reader.next().await.unwrap().is_none());
}

#[cfg(feature = "csv")]
#[tokio::test]
async fn reserved_data_names_conflict_even_when_projected_out() {
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_file_format::csv::CsvAccessor;
    let source = CsvAccessor::infer(MemoryAccessor::new(b"_source_object,id\nold,1\n", 1));
    let request = ReadRequest::new("data.csv").with_options(ReadOptions {
        source_columns: vec![SourceColumn::Object],
        projection: Some(vec!["id".into()]),
        ..Default::default()
    });
    assert_eq!(
        source.open(request.clone()).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        source.plan_scan(request).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
}
