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
use parquet::arrow::ArrowWriter;

#[tokio::test]
async fn json_directory_union_by_name_aligns_nested_struct_and_list_fields() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.jsonl"), b"{\"id\":1,\"all_null\":null,\"meta\":{\"left\":\"a\"},\"items\":[{\"x\":1}]}\n{\"id\":2,\"meta\":null,\"items\":[]}\n").unwrap();
    std::fs::write(
        dir.path().join("b.jsonl"),
        b"{\"id\":3,\"all_null\":3,\"meta\":{\"right\":true},\"items\":[{\"y\":\"b\"}],\"tail\":\"last\"}\n",
    )
    .unwrap();
    for native in [false, true] {
        let storage = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(dir.path()));
        let directory = DirectoryAccessor::new(storage.clone(), JsonAccessor::infer(storage))
            .with_union_by_name();
        let mut request = DirectoryRequest::new("");
        request.suffix = Some(".jsonl".into());
        let mut reader = if native {
            directory
                .with_file_planning()
                .layer(ParallelReadLayer::new(
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroUsize::MIN,
                ))
                .open(request)
                .await
                .unwrap()
        } else {
            directory.open(request).await.unwrap()
        };
        let schema = reader.metadata().schema.clone();
        assert_eq!(
            schema.field_with_name("all_null").unwrap().data_type(),
            &DataType::Int64
        );
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
        let mut missing_tail = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), schema);
            let values = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>();
            let metas = batch.column_by_name("meta").unwrap().as_struct();
            let items = batch.column_by_name("items").unwrap().as_list::<i32>();
            missing_tail += batch.column_by_name("tail").unwrap().null_count();
            assert_eq!(
                batch.column_by_name("all_null").unwrap().null_count(),
                if values.value(0) == 1 {
                    batch.num_rows()
                } else {
                    0
                }
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
        assert_eq!(missing_tail, 2);
    }
}

#[tokio::test]
async fn union_by_name_projection_can_select_a_column_missing_from_the_first_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.jsonl"), b"{\"id\":1}\n").unwrap();
    std::fs::write(
        dir.path().join("b.jsonl"),
        b"{\"tail\":\"last\",\"id\":2}\n",
    )
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
        let file = std::fs::File::create(dir.path().join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
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
        .open(FileSetRequest::new(["a.parquet", "b.parquet", "a.parquet"]))
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
    let mut missing_left = 0;
    let mut missing_right = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        ids.extend_from_slice(batch.column(0).as_primitive::<Int64Type>().values());
        missing_left += batch.column(1).null_count();
        missing_right += batch.column(2).null_count();
    }
    assert_eq!(ids, [1, 2, 3, 1, 2]);
    assert_eq!(missing_left, 1);
    assert_eq!(missing_right, 4);
}
