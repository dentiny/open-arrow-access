use std::{io::Write, num::NonZeroUsize, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, cast::AsArray, types::Int64Type};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ReadOptions, ReadRequest, Reader, RecordReader, ScanPlanner,
};
use open_arrow_access_dataset::{
    directory::{DirectoryAccessor, DirectoryRequest},
    file_set::{FileSetAccessor, FileSetRequest},
};
use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
use open_arrow_access_file_format::{
    csv::CsvAccessor, json::JsonAccessor, parquet::ParquetAccessor,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::MemoryAccessor;
use parquet::arrow::AsyncArrowWriter;

// Exercise both ordinary open and native file planning, including lazy execution.
async fn check_empty_inference<A>(source: A, compressed: bool)
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + ScanPlanner + Clone + 'static,
{
    for name in ["empty", "whitespace"] {
        assert!(
            source
                .open_optional(ReadRequest::new(name))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            source
                .plan_scan_optional(ReadRequest::new(name))
                .await
                .unwrap()
                .is_none()
        );
    }
    let error = source.open(ReadRequest::new("empty")).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        FileSetAccessor::new(source.clone())
            .open(FileSetRequest::new(["empty", "data"]))
            .await
            .is_err()
    );
    for native_planning in [false, true] {
        let files = FileSetAccessor::new(source.clone()).with_union_by_name();
        let files = if native_planning {
            files.with_file_planning()
        } else {
            files
        };
        // Empty files before, between, and after data must contribute no rows.
        for projection in [None, Some(vec!["id".into()]), Some(vec![])] {
            let request = FileSetRequest::new(["empty", "data", "whitespace", "data", "empty"])
                .with_options(ReadOptions {
                    projection: projection.clone(),
                    ..Default::default()
                });
            let mut reader = files.open(request).await.unwrap();
            assert_eq!(
                reader.metadata().schema.fields().len(),
                projection.as_ref().map_or(1, Vec::len)
            );
            let mut rows = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                assert_eq!(batch.schema(), reader.metadata().schema);
                if batch.num_columns() > 0 {
                    assert_eq!(batch.column(0).as_primitive::<Int64Type>().values(), &[1]);
                }
                rows += batch.num_rows();
            }
            assert_eq!(rows, 2);
        }
        let error = files
            .open(FileSetRequest::new(["empty", "whitespace"]))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        for invalid in ["invalid", "missing"]
            .into_iter()
            .chain(compressed.then_some("corrupt"))
        {
            let error = files
                .open(FileSetRequest::new(["empty", "data", invalid]))
                .await
                .err()
                .unwrap();
            assert!(matches!(
                error.kind(),
                ErrorKind::Decode | ErrorKind::InvalidInput | ErrorKind::NotFound
            ));
            assert!(
                source
                    .open_optional(ReadRequest::new(invalid))
                    .await
                    .is_err()
            );
            assert!(
                source
                    .plan_scan_optional(ReadRequest::new(invalid))
                    .await
                    .is_err()
            );
        }
        let error = files
            .open(FileSetRequest::new(Vec::<String>::new()))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn inferred_union_skips_empty_csv_and_json_including_gzip() {
    for json in [false, true] {
        for compressed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (data, invalid) = if json {
                (b"{\"id\":1}\n".as_slice(), b"{".as_slice())
            } else {
                (b"id\n1\n".as_slice(), b"id,id\n1,2\n".as_slice())
            };
            for (name, bytes) in [
                ("empty", b"".as_slice()),
                ("whitespace", b"\n\r\n".as_slice()),
                ("data", data),
                ("invalid", invalid),
            ] {
                let bytes = if compressed {
                    let mut encoder =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(bytes).unwrap();
                    encoder.finish().unwrap()
                } else {
                    bytes.to_vec()
                };
                tokio::fs::write(dir.path().join(name), bytes)
                    .await
                    .unwrap();
            }
            let storage =
                OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
            if compressed {
                // A corrupt compressed stream is not an empty input.
                tokio::fs::write(dir.path().join("corrupt"), b"bad gzip")
                    .await
                    .unwrap();
                let storage = storage.layer(DecompressionLayer::new(Gzip));
                if json {
                    check_empty_inference(JsonAccessor::infer(storage), compressed).await;
                } else {
                    check_empty_inference(CsvAccessor::infer(storage), compressed).await;
                }
            } else if json {
                check_empty_inference(JsonAccessor::infer(storage), compressed).await;
            } else {
                check_empty_inference(CsvAccessor::infer(storage), compressed).await;
            }
        }
    }
}

async fn check_known_schema_empty<A>(source: A)
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + ScanPlanner,
{
    let mut reader = source
        .open_optional(ReadRequest::new("empty"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 1);
    assert!(reader.next().await.unwrap().is_none());
    let plan = source
        .plan_scan_optional(ReadRequest::new("empty"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(plan.metadata.schema.fields().len(), 1);
    assert!(plan.into_reader().next().await.unwrap().is_none());
}

#[tokio::test]
async fn optional_open_and_plan_preserve_known_schema_empty_inputs() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    check_known_schema_empty(CsvAccessor::new(
        MemoryAccessor::new(b"", 1),
        schema.clone(),
    ))
    .await;
    check_known_schema_empty(JsonAccessor::new(MemoryAccessor::new(b"", 1), schema)).await;
}

#[tokio::test]
async fn input_becoming_empty_after_preflight_is_a_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("data"), b"{\"id\":1}")
        .await
        .unwrap();
    let storage = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let files = FileSetAccessor::new(JsonAccessor::infer(storage)).with_union_by_name();
    let mut reader = files.open(FileSetRequest::new(["data"])).await.unwrap();
    tokio::fs::write(dir.path().join("data"), b"")
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("schema differs"));
}

#[tokio::test]
async fn zero_column_json_records_are_not_skipped_as_empty_inputs() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("no-fields"), b"{}\n{}")
        .await
        .unwrap();
    tokio::fs::write(dir.path().join("data"), b"{\"id\":1}")
        .await
        .unwrap();
    let storage = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let files = FileSetAccessor::new(JsonAccessor::infer(storage)).with_union_by_name();
    let mut reader = files
        .open(FileSetRequest::new(["no-fields", "data"]))
        .await
        .unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.extend(batch.column(0).as_primitive::<Int64Type>().iter());
    }
    assert_eq!(ids, [None, None, Some(1)]);
}

#[tokio::test]
async fn union_projection_skips_invalid_unselected_json_values() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("unused", DataType::Int64, false),
    ]));
    let source = JsonAccessor::new(
        MemoryAccessor::new(b"{\"id\":1,\"unused\":\"invalid\"}\n", 3),
        schema,
    );
    let files = FileSetAccessor::new(source).with_union_by_name();
    let request = FileSetRequest::new(["a"]).with_options(ReadOptions {
        projection: Some(vec!["id".into()]),
        ..Default::default()
    });
    let mut reader = files.open(request).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.column(0).as_primitive::<Int64Type>().values(), &[1]);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn json_directory_union_by_name_aligns_nested_struct_and_list_fields() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["0-empty.jsonl", "z-empty.jsonl"] {
        tokio::fs::write(dir.path().join(name), b"").await.unwrap();
    }
    tokio::fs::write(dir.path().join("a.jsonl"), b"{\"id\":1,\"all_null\":null,\"meta\":{\"left\":\"a\"},\"items\":[{\"x\":1}]}\n{\"id\":2,\"meta\":null,\"items\":[]}\n").await.unwrap();
    tokio::fs::write(
        dir.path().join("b.jsonl"),
        b"{\"id\":3,\"all_null\":3,\"meta\":{\"right\":true},\"items\":[{\"y\":\"b\"}]}\n",
    )
    .await
    .unwrap();
    let storage = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let directory =
        DirectoryAccessor::new(storage.clone(), JsonAccessor::infer(storage)).with_union_by_name();
    let mut request = DirectoryRequest::new("");
    request.suffix = Some(".jsonl".into());
    let mut reader = directory.open(request).await.unwrap();
    let schema = reader.metadata().schema.clone();
    let DataType::Struct(meta) = schema.field_with_name("meta").unwrap().data_type() else {
        panic!("expected struct");
    };
    assert_eq!(
        meta.iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [("left", true), ("right", true)]
    );
    let mut ids = Vec::new();
    let mut null_values = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        let values = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let metas = batch.column_by_name("meta").unwrap().as_struct();
        let items = batch.column_by_name("items").unwrap().as_list::<i32>();
        null_values.extend(
            batch
                .column_by_name("all_null")
                .unwrap()
                .as_primitive::<Int64Type>()
                .iter(),
        );
        for row in 0..batch.num_rows() {
            let id = values.value(row);
            ids.push(id);
            let children = items.value(row);
            let children = children.as_struct();
            match id {
                1 => {
                    assert_eq!(
                        metas
                            .column_by_name("left")
                            .unwrap()
                            .as_string::<i32>()
                            .value(row),
                        "a"
                    );
                    assert!(metas.column_by_name("right").unwrap().is_null(row));
                    assert_eq!(
                        children
                            .column_by_name("x")
                            .unwrap()
                            .as_primitive::<Int64Type>()
                            .value(0),
                        1
                    );
                    assert!(children.column_by_name("y").unwrap().is_null(0));
                }
                2 => {
                    assert!(metas.is_null(row));
                    assert_eq!(items.value_length(row), 0);
                }
                3 => {
                    assert!(metas.column_by_name("left").unwrap().is_null(row));
                    assert!(
                        metas
                            .column_by_name("right")
                            .unwrap()
                            .as_boolean()
                            .value(row)
                    );
                    assert!(children.column_by_name("x").unwrap().is_null(0));
                    assert_eq!(
                        children
                            .column_by_name("y")
                            .unwrap()
                            .as_string::<i32>()
                            .value(0),
                        "b"
                    );
                }
                _ => unreachable!(),
            }
        }
    }
    assert_eq!(ids, [1, 2, 3]);
    assert_eq!(null_values, [None, None, Some(3)]);
}

#[tokio::test]
async fn union_by_name_projection_can_select_a_column_missing_from_the_first_file() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("a.jsonl"), b"{\"id\":1}\n")
        .await
        .unwrap();
    tokio::fs::write(
        dir.path().join("b.jsonl"),
        b"{\"tail\":\"last\",\"id\":2}\n",
    )
    .await
    .unwrap();
    let storage = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let files = FileSetAccessor::new(JsonAccessor::infer(storage)).with_union_by_name();
    for columns in [vec!["tail".to_owned(), "id".to_owned()], vec![]] {
        let request = FileSetRequest::new(["a.jsonl", "b.jsonl"]).with_options(ReadOptions {
            projection: Some(columns.clone()),
            ..Default::default()
        });
        let mut reader = files.open(request).await.unwrap();
        assert_eq!(
            reader
                .metadata()
                .schema
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            columns.iter().collect::<Vec<_>>()
        );
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            if !columns.is_empty() && rows == 0 {
                assert!(batch.column(0).is_null(0));
            }
            rows += batch.num_rows();
        }
        assert_eq!(rows, 2);
    }
}

#[tokio::test]
async fn parquet_union_by_name_only_relaxes_missing_fields() {
    let dir = tempfile::tempdir().unwrap();
    let batches = [
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("left", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("right", DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["r"])),
                Arc::new(Int64Array::from(vec![3])),
            ],
        )
        .unwrap(),
    ];
    for (name, batch) in ["a.parquet", "b.parquet"].into_iter().zip(batches) {
        let file = tokio::fs::File::create(dir.path().join(name))
            .await
            .unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
    }
    let storage = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
    let files = FileSetAccessor::new(ParquetAccessor::new(storage))
        .with_union_by_name()
        .with_file_planning();
    let mut reader = files
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .open(FileSetRequest::new(["a.parquet", "b.parquet"]))
        .await
        .unwrap();
    let schema = reader.metadata().schema.clone();
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [("id", false), ("left", true), ("right", true)]
    );
    let mut ids = Vec::new();
    let mut left_values = Vec::new();
    let mut right_values = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.extend_from_slice(batch.column(0).as_primitive::<Int64Type>().values());
        left_values.extend(batch.column(1).as_primitive::<Int64Type>().iter());
        right_values.extend(
            batch
                .column(2)
                .as_string::<i32>()
                .iter()
                .map(|value| value.map(str::to_owned)),
        );
    }
    assert_eq!(ids, [1, 2, 3]);
    assert_eq!(left_values, [Some(10), Some(20), None]);
    assert_eq!(right_values, [None, None, Some("r".to_owned())]);
}
