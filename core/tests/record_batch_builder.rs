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

#[test]
fn wide_projected_rows_and_nested_lists_reuse_correct_field_positions() {
    let names = (0..1024).map(|i| format!("c{i}")).collect::<Vec<_>>();
    let nested_names = (0..512).map(|i| format!("n{i}")).collect::<Vec<_>>();
    let mut fields = names
        .iter()
        .map(|name| Field::new(name, DataType::UInt64, false))
        .collect::<Vec<_>>();
    fields.push(Field::new(
        "nested",
        list(
            DataType::Struct(
                nested_names
                    .iter()
                    .map(|name| Field::new(name, DataType::UInt64, false))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            false,
        ),
        false,
    ));
    let schema = Arc::new(Schema::new(fields));
    let mut builder = RecordBatchBuilder::new(
        schema,
        Some(vec!["c1023".into(), "nested".into(), "c0".into()]),
    )
    .unwrap();
    for row_number in 0..3u64 {
        builder
            .push(|row| {
                for (i, name) in names.iter().enumerate() {
                    row.append(name, row_number * 10_000 + i as u64)?;
                }
                row.append_structs("nested", 0..2u64, |row, entry| {
                    for (i, name) in nested_names.iter().enumerate().rev() {
                        row.append(name, row_number * 10_000 + entry * 1000 + i as u64)?;
                    }
                    Ok(())
                })
            })
            .unwrap();
    }
    let batch = builder.finish().unwrap();
    assert_eq!(batch.num_rows(), 3);
    for row in 0..3 {
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(row),
            row as u64 * 10_000 + 1023
        );
        assert_eq!(
            batch
                .column(2)
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(row),
            row as u64 * 10_000
        );
        let entries = batch.column(1).as_list::<i32>().value(row);
        let entries = entries.as_struct();
        for i in [0, 511] {
            let values = entries
                .column(i)
                .as_primitive::<arrow_array::types::UInt64Type>();
            for entry in 0..2 {
                assert_eq!(
                    values.value(entry),
                    row as u64 * 10_000 + entry as u64 * 1000 + i as u64
                );
            }
        }
    }
}

#[test]
fn resolved_output_schema_must_contain_unique_unchanged_fields() {
    let field = Field::new("id", DataType::UInt64, false);
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    for fields in [
        vec![field.clone(), field.clone()],
        vec![Field::new("unknown", DataType::UInt64, false)],
        vec![Field::new("id", DataType::Utf8, false)],
        vec![
            field
                .clone()
                .with_metadata(HashMap::from([("changed".into(), "yes".into())])),
        ],
    ] {
        let error =
            RecordBatchBuilder::build_row(schema.clone(), Arc::new(Schema::new(fields)), |_| {
                panic!("invalid schema reached the writer")
            })
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    for projection in [None, Some(vec![])] {
        let mut builder = RecordBatchBuilder::new(schema.clone(), projection).unwrap();
        let error = builder.push(|row| row.append("unknown", 1u64)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(
            builder.finish().unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[test]
fn cached_nested_layouts_preserve_null_parents_and_required_children() {
    let inner = DataType::Struct(vec![Field::new("id", DataType::UInt64, false)].into());
    let group = DataType::Struct(vec![Field::new("inner", inner, false)].into());
    let schema = Arc::new(Schema::new(vec![Field::new("group", group, true)]));
    let mut builder = RecordBatchBuilder::new(schema, None).unwrap();
    builder.push(|_| Ok(())).unwrap();
    builder
        .push(|row| {
            row.append_struct("group", |row| {
                row.append_struct("inner", |row| row.append("id", 7u64))
            })
        })
        .unwrap();
    let batch = builder.finish().unwrap();
    let groups = batch["group"].as_struct();
    assert!(groups.is_null(0));
    assert!(!groups.is_null(1));
    let ids = groups
        .column(0)
        .as_struct()
        .column(0)
        .as_primitive::<arrow_array::types::UInt64Type>();
    assert_eq!(ids.value(1), 7);
}
