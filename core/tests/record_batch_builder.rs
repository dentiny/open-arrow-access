use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array,
    cast::AsArray,
    types::{Date32Type, UInt32Type, UInt64Type},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{ErrorKind, FieldValue, RecordBatchBuilder, RecordBatchRow, Result};

fn list(data_type: DataType, large: bool) -> DataType {
    let item = Arc::new(Field::new("item", data_type, false));
    if large {
        DataType::LargeList(item)
    } else {
        DataType::List(item)
    }
}

fn book_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, false)
                .with_metadata(HashMap::from([("upstream".into(), "book_id".into())])),
            Field::new("title", DataType::LargeUtf8, true),
            Field::new("issued", DataType::Date32, true),
            Field::new(
                "files",
                list(
                    DataType::Struct(
                        vec![
                            Field::new("uri", DataType::Utf8, false),
                            Field::new("format", list(DataType::Utf8, false), false),
                            Field::new("data", DataType::LargeBinary, false),
                        ]
                        .into(),
                    ),
                    true,
                ),
                false,
            ),
        ],
        HashMap::from([("snapshot".into(), "fixture".into())]),
    ))
}

#[test]
fn builds_gutenberg_nested_files_without_changing_binary_or_schema() {
    let schema = book_schema();
    let mut builder = RecordBatchBuilder::new(schema.clone(), None).unwrap();
    assert!(builder.is_empty());
    builder
        .push(|row| {
            row.append("id", 42u64)?;
            row.append("title", "A & B")?;
            row.append("issued", 1i32)?;
            row.append_structs("files", ["book.zip"], |row, uri| {
                row.append("uri", uri)?;
                row.append_list("format", ["application/zip", "text/plain"])?;
                row.append("data", [0, 255, 128, 13, 10].as_slice())
            })
        })
        .unwrap();
    builder
        .push(|row| {
            row.append("id", 43u64)?;
            row.append_structs("files", std::iter::empty::<()>(), |_, _| unreachable!())
        })
        .unwrap();
    assert_eq!(builder.len(), 2);
    let batch = builder.finish().unwrap();
    assert_eq!(batch.schema(), schema);
    assert_eq!(
        batch.column(0).as_primitive::<UInt64Type>().values(),
        &[42, 43]
    );
    assert_eq!(batch.column(2).as_primitive::<Date32Type>().value(0), 1);
    assert!(batch.column(1).is_null(1));
    let files = batch.column(3).as_list::<i64>();
    assert_eq!(files.value(1).len(), 0);
    let first = files.value(0);
    let file = first.as_struct();
    assert_eq!(file.column(0).as_string::<i32>().value(0), "book.zip");
    assert_eq!(file.column(1).as_list::<i32>().value(0).len(), 2);
    assert_eq!(
        file.column(2).as_binary::<i64>().value(0),
        &[0, 255, 128, 13, 10]
    );
}

#[test]
fn builds_pmc_scalars_and_preserves_null_versus_empty_media_urls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("pmcid", DataType::Utf8, false),
        Field::new("version", DataType::UInt32, true),
        Field::new("is_openaccess", DataType::Boolean, true),
        Field::new("media_urls", list(DataType::Utf8, false), true),
    ]));
    let mut builder = RecordBatchBuilder::new(schema, None).unwrap();
    builder
        .push(|row| {
            row.append("pmcid", "PMC1")?;
            row.append("version", Some(1u32))?;
            row.append("is_openaccess", Some(true))?;
            row.append_list("media_urls", ["image.png"])
        })
        .unwrap();
    builder
        .push(|row| {
            row.append("pmcid", "PMC2")?;
            row.append_list("media_urls", std::iter::empty::<&str>())
        })
        .unwrap();
    builder
        .push(|row| {
            row.append("pmcid", "PMC3")?;
            row.append("media_urls", FieldValue::Null)
        })
        .unwrap();
    let batch = builder.finish().unwrap();
    assert_eq!(batch.column(1).as_primitive::<UInt32Type>().value(0), 1);
    assert!(batch.column(2).as_boolean().value(0));
    let urls = batch.column(3).as_list::<i32>();
    assert_eq!(urls.value(0).as_string::<i32>().value(0), "image.png");
    assert!(!urls.is_null(1));
    assert_eq!(urls.value(1).len(), 0);
    assert!(urls.is_null(2));
}

#[test]
fn projection_skips_list_callbacks_and_retains_zero_column_rows() {
    for columns in [vec!["title".into(), "id".into()], vec![]] {
        let mut builder = RecordBatchBuilder::new(book_schema(), Some(columns.clone())).unwrap();
        for id in 0..2u64 {
            builder
                .push(|row| {
                    row.append("id", id)?;
                    row.append("title", "example")?;
                    row.append_structs("files", [()], |_, _| panic!("unselected callback"))
                })
                .unwrap();
        }
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            columns
        );
        assert_eq!(batch.schema().metadata(), book_schema().metadata());
        if !columns.is_empty() {
            assert_eq!(batch.column(0).as_string::<i64>().value(0), "example");
            assert_eq!(
                batch.schema().field(1).metadata(),
                book_schema().field(0).metadata()
            );
        }
    }
    let builder = RecordBatchBuilder::new(book_schema(), None).unwrap();
    let schema = builder.schema().clone();
    let batch = builder.finish().unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), schema);
}

#[test]
fn errors_invalidate_partial_rows_even_if_a_callback_ignores_an_error() {
    for columns in [vec!["unknown".into()], vec!["id".into(), "id".into()]] {
        assert_eq!(
            RecordBatchBuilder::new(book_schema(), Some(columns))
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    type Write = fn(&mut RecordBatchRow<'_>) -> Result<()>;
    let failures: [Write; 5] = [
        |row| row.append("unknown", 1u64),
        |row| row.append("id", "wrong type"),
        |row| {
            row.append("id", 1u64)?;
            row.append("id", 2u64)
        },
        |row| {
            let _ = row.append("id", "ignored mismatch");
            Ok(())
        },
        |_| Ok(()),
    ];
    for write in failures {
        let mut builder = RecordBatchBuilder::new(book_schema(), Some(vec!["id".into()])).unwrap();
        assert!(builder.push(write).is_err());
        assert_eq!(
            builder.finish().unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    let unsupported = Arc::new(Schema::new(vec![Field::new(
        "float",
        DataType::Float64,
        true,
    )]));
    assert_eq!(
        RecordBatchBuilder::new(unsupported, None)
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
}

#[test]
fn null_structs_and_nested_failures_keep_child_builders_consistent() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        DataType::Struct(vec![Field::new("id", DataType::UInt64, false)].into()),
        true,
    )]));
    let mut builder = RecordBatchBuilder::new(schema.clone(), None).unwrap();
    builder
        .push(|row| row.append("nested", FieldValue::Null))
        .unwrap();
    builder
        .push(|row| row.append_struct("nested", |row| row.append("id", 1u64)))
        .unwrap();
    let batch = builder.finish().unwrap();
    assert!(batch.column(0).is_null(0));
    assert_eq!(batch.schema(), schema);

    let mut builder = RecordBatchBuilder::new(book_schema(), Some(vec!["files".into()])).unwrap();
    assert!(
        builder
            .push(|row| row.append_structs("files", [()], |row, _| {
                row.append("uri", "partial")?;
                let _ = row.append("data", 1u64);
                Ok(())
            }))
            .is_err()
    );
    assert!(builder.finish().is_err());
}
