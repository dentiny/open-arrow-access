#![cfg(feature = "warc")]
use arrow_array::{StringArray, StringViewArray, UInt64Array};
use open_arrow_access_core::ScanPlanner;
use open_arrow_access_core::{
    Accessor, AccessorExt, FactoryAccessor, ReadOptions, ReadRequest, RecordReader,
};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_decompression::AutoDecompressionLayer;
use open_arrow_access_file_format::warc::WarcArrowFormat;
use open_arrow_access_layers::{
    ColumnEvolution, ParallelReadLayer, SchemaEvolutionLayer, SourceColumns, SourcePositionLayer,
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
async fn rows(mut reader: Box<dyn RecordReader>) -> Vec<Row> {
    let mut result = Vec::new();
    let schema = reader.metadata().schema.clone();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        let offsets = batch
            .column_by_name("offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let decoded = batch
            .column_by_name("decoded")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let objects = batch
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column_by_name("record_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(ids.value(i), format!("<urn:test:{}>", result.len()));
            result.push((
                objects.value(i).to_owned(),
                offsets.value(i),
                decoded.value(i),
            ));
        }
    }
    result
}
fn columns() -> SourceColumns {
    SourceColumns::new()
        .with_object("path")
        .with_offset("offset")
        .with_offset_in_frame("decoded")
}
#[tokio::test]
async fn record_coordinates_survive_projection_parallelism_and_resume() {
    let records = records();
    let members = records.iter().map(|r| gzip(r)).collect::<Vec<_>>();
    for mode in 0..3 {
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
        let source = columns().wrap(FactoryAccessor::new(
            MemoryAccessor::new(&input, 7).layer(SourcePositionLayer::default()),
            WarcArrowFormat.with_source_positions(),
        ));
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
            rows(source.open(request.clone()).await.unwrap()).await,
            expected
        );
        let source = source.layer(ParallelReadLayer::new(
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::MIN,
        ));
        assert_eq!(rows(source.open(request).await.unwrap()).await, expected);
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
async fn records_spanning_compression_members_keep_their_start_coordinates() {
    let mut input = gzip(b" \t\r\n");
    let mut expected = Vec::new();
    for record in records() {
        expected.push(("data.warc.gz".to_owned(), input.len() as u64, 0));
        let split = record.len() / 2;
        input.extend(gzip(&record[..split]));
        input.extend(gzip(&record[split..]));
    }
    let source = columns().wrap(FactoryAccessor::new(
        MemoryAccessor::new(&input, 7).layer(SourcePositionLayer::default()),
        WarcArrowFormat.with_source_positions(),
    ));
    assert_eq!(
        rows(source.open(ReadRequest::new("data.warc.gz")).await.unwrap()).await,
        expected
    );
}

#[tokio::test]
async fn file_set_union_parallel_and_generated_columns_compose() {
    let records = records();
    let input = gzip(&records.concat());
    let decoded_offsets = [0, records[0].len(), records[0].len() + records[1].len()];
    let objects = ["first.warc.gz", "second.warc.gz"];
    let expected = objects
        .iter()
        .flat_map(|path| {
            decoded_offsets
                .iter()
                .map(move |decoded| (path.to_string(), format!("{path}:{decoded}"), 0u64))
        })
        .collect::<Vec<_>>();
    for union in [false, true] {
        let format = columns().wrap(FactoryAccessor::new(
            MemoryAccessor::new(&input, 7).layer(SourcePositionLayer::default()),
            WarcArrowFormat.with_source_positions(),
        ));
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
                SchemaEvolutionLayer::try_new(vec![
                    ColumnEvolution::add_column_sql(
                        "docid",
                        "concat(path, ':', cast(decoded as varchar))",
                    ),
                    ColumnEvolution::remove_column("decoded"),
                ])
                .unwrap(),
            );
        let mut reader = source.open(FileSetRequest::new(objects)).await.unwrap();
        assert!(reader.metadata().schema.index_of("decoded").is_err());
        let mut actual = Vec::new();
        while let Some(batch) = reader.next().await.unwrap() {
            let paths = batch
                .column_by_name("path")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let offsets = batch
                .column_by_name("offset")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let ids = batch.column_by_name("docid").unwrap();
            for i in 0..batch.num_rows() {
                let id = if let Some(ids) = ids.as_any().downcast_ref::<StringArray>() {
                    ids.value(i)
                } else {
                    ids.as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap()
                        .value(i)
                };
                actual.push((paths.value(i).to_owned(), id.to_owned(), offsets.value(i)));
            }
        }
        assert_eq!(actual, expected);
    }
}
#[tokio::test]
async fn stored_offsets_reject_predecoded_input() {
    use open_arrow_access_core::ErrorKind;
    let input = gzip(&records().concat());
    let source = MemoryAccessor::new(&input, 3)
        .layer(AutoDecompressionLayer::new())
        .layer(SourcePositionLayer::default());
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
    use open_arrow_access_file_format::csv::CsvFileFormat;
    let input = MemoryAccessor::new(b"id\n1\n2\n", 1);
    let observations = input.observations.clone();
    let source = SourceColumns::new()
        .with_offset("offset")
        .wrap(FactoryAccessor::new(input.clone(), CsvFileFormat::infer()));
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
    let source = SourceColumns::new()
        .with_object("path")
        .wrap(FactoryAccessor::new(input, CsvFileFormat::infer()));
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
    let source = SourceColumns::new()
        .with_offset("record_id")
        .wrap(FactoryAccessor::new(
            MemoryAccessor::new(&records().concat(), 3),
            WarcArrowFormat.with_source_positions(),
        ));
    assert_eq!(
        source
            .open(ReadRequest::new("data.warc"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
    let source = columns().wrap(FactoryAccessor::new(
        MemoryAccessor::new(&gzip(b""), 1).layer(SourcePositionLayer::default()),
        WarcArrowFormat.with_source_positions(),
    ));
    let mut reader = source
        .open(ReadRequest::new("empty.warc.gz"))
        .await
        .unwrap();
    assert!(reader.metadata().schema.index_of("offset").is_ok());
    assert!(reader.next().await.unwrap().is_none());
}
