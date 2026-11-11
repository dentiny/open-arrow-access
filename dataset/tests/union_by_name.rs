use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, cast::AsArray, types::Int64Type};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Accessor, AccessorExt, ReadOptions};
use open_arrow_access_dataset::{
    directory::{DirectoryAccessor, DirectoryRequest},
    file_set::{FileSetAccessor, FileSetRequest},
};
use open_arrow_access_file_format::{json::JsonAccessor, parquet::ParquetAccessor};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_storage::OpenDalAccessor;
use parquet::arrow::AsyncArrowWriter;

#[tokio::test]
async fn json_directory_union_by_name_aligns_nested_struct_and_list_fields() {
    let dir = tempfile::tempdir().unwrap();
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
