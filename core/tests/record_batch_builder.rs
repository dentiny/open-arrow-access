use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{ErrorKind, FieldValue, RecordBatchBuilder};

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
            Field::new("id", DataType::UInt64, false),
            Field::new("title", DataType::LargeUtf8, true),
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
fn gutenberg_files_preserve_nested_values_binary_and_schema() {
    let schema = book_schema();
    let mut builder = RecordBatchBuilder::new(schema.clone(), None).unwrap();
    builder
        .push(|row| {
            row.append("id", 42u64)?;
            row.append_structs("files", ["book.zip"], |row, uri| {
                row.append("uri", uri)?;
                row.append_list("format", ["application/zip", "text/plain"])?;
                row.append("data", [0, 255, 128].as_slice())
            })
        })
        .unwrap();
    let batch = builder.finish().unwrap();
    assert_eq!(batch.schema(), schema);
    assert!(batch.column(1).is_null(0));
    let files = batch.column(2).as_list::<i64>().value(0);
    let file = files.as_struct();
    assert_eq!(file.column(0).as_string::<i32>().value(0), "book.zip");
    assert_eq!(file.column(1).as_list::<i32>().value(0).len(), 2);
    assert_eq!(file.column(2).as_binary::<i64>().value(0), &[0, 255, 128]);
}

#[test]
fn pmc_preserves_null_empty_and_populated_media_urls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("version", DataType::UInt32, true),
        Field::new("is_openaccess", DataType::Boolean, true),
        Field::new("media_urls", list(DataType::Utf8, false), true),
    ]));
    let mut builder = RecordBatchBuilder::new(schema, None).unwrap();
    for urls in [None, Some([].as_slice()), Some(["image.png"].as_slice())] {
        builder
            .push(|row| {
                row.append("version", Some(1u32))?;
                row.append("is_openaccess", true)?;
                match urls {
                    None => row.append("media_urls", FieldValue::Null),
                    Some(urls) => row.append_list("media_urls", urls.iter().copied()),
                }
            })
            .unwrap();
    }
    let batch = builder.finish().unwrap();
    let urls = batch.column(2).as_list::<i32>();
    assert!(urls.is_null(0));
    assert!(!urls.is_null(1));
    assert_eq!(urls.value(1).len(), 0);
    assert_eq!(urls.value(2).as_string::<i32>().value(0), "image.png");
}

#[test]
fn projection_preserves_order_and_row_counts_without_reading_files() {
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
    }
}

#[test]
fn nested_append_errors_prevent_emitting_partial_rows() {
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
    assert_eq!(
        builder.finish().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
