use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array,
    cast::AsArray,
    types::{Date32Type, UInt64Type},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use open_arrow_access_core::{ErrorKind, RecordBatchBuilder};
use serde::Serialize;

#[derive(Serialize)]
struct File<'a> {
    uri: &'a str,
    format: Vec<&'a str>,
    data: Vec<u8>,
}

#[derive(Serialize)]
struct Record<'a, F> {
    id: u64,
    title: Option<&'a str>,
    issued: Option<i32>,
    files: F,
}

fn schema() -> SchemaRef {
    let formats = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let files = DataType::LargeList(Arc::new(Field::new(
        "item",
        DataType::Struct(
            vec![
                Field::new("uri", DataType::Utf8, false),
                Field::new("format", formats, false),
                Field::new("data", DataType::LargeBinary, false),
            ]
            .into(),
        ),
        false,
    )));
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::UInt64, false)
                .with_metadata(HashMap::from([("upstream".into(), "book_id".into())])),
            Field::new("title", DataType::LargeUtf8, true),
            Field::new("issued", DataType::Date32, true),
            Field::new("files", files, false),
        ],
        HashMap::from([("snapshot".into(), "fixture".into())]),
    ))
}

#[test]
fn encodes_nested_records_and_raw_binary_with_schema_metadata() {
    let schema = schema();
    let mut builder = RecordBatchBuilder::new(schema.clone(), None).unwrap();
    assert!(builder.is_empty());
    builder
        .push(&Record {
            id: 42,
            title: Some("A & B"),
            issued: Some(1),
            files: vec![File {
                uri: "book.zip",
                format: vec!["application/zip", "text/plain"],
                data: vec![0, 255, 128, 13, 10],
            }],
        })
        .unwrap();
    builder
        .push(&Record {
            id: 43,
            title: None,
            issued: None,
            files: Vec::<File<'_>>::new(),
        })
        .unwrap();
    assert_eq!(builder.len(), 2);
    let batch = builder.finish().unwrap();
    assert_eq!(batch.schema(), schema);
    assert_eq!(batch.num_rows(), 2);
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

struct Unserializable;

impl Serialize for Unserializable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "unselected payload must not be serialized",
        ))
    }
}

#[test]
fn projection_reorders_fields_and_skips_unselected_payloads() {
    let schema = schema();
    let mut builder =
        RecordBatchBuilder::new(schema.clone(), Some(vec!["title".into(), "id".into()])).unwrap();
    assert_eq!(builder.schema().fields()[0].name(), "title");
    builder
        .push(&Record {
            id: 7,
            title: Some("example"),
            issued: None,
            files: Unserializable,
        })
        .unwrap();
    let batch = builder.finish().unwrap();
    assert_eq!(batch.schema().metadata(), schema.metadata());
    assert_eq!(
        batch.schema().field(1).metadata(),
        schema.field(0).metadata()
    );
    assert_eq!(batch.column(0).as_string::<i64>().value(0), "example");
    assert_eq!(batch.column(1).as_primitive::<UInt64Type>().value(0), 7);
}

#[test]
fn preserves_empty_batches_and_zero_column_row_counts() {
    for projection in [None, Some(vec![])] {
        let mut builder = RecordBatchBuilder::new(schema(), projection.clone()).unwrap();
        let expected = builder.schema().clone();
        let empty = builder.finish().unwrap();
        assert_eq!(empty.schema(), expected);
        assert_eq!(empty.num_rows(), 0);
        if projection.is_some() {
            builder = RecordBatchBuilder::new(schema(), projection).unwrap();
            for id in 0..2 {
                builder
                    .push(&Record {
                        id,
                        title: None,
                        issued: None,
                        files: Unserializable,
                    })
                    .unwrap();
            }
            let batch = builder.finish().unwrap();
            assert_eq!(batch.num_columns(), 0);
            assert_eq!(batch.num_rows(), 2);
        }
    }
}

#[test]
fn rejects_invalid_projection_and_never_returns_partially_appended_rows() {
    for projection in [vec!["unknown".into()], vec!["id".into(), "id".into()]] {
        assert_eq!(
            RecordBatchBuilder::new(schema(), Some(projection))
                .err()
                .unwrap()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    let mut builder =
        RecordBatchBuilder::new(schema(), Some(vec!["id".into(), "files".into()])).unwrap();
    builder
        .push(&Record {
            id: 1,
            title: None,
            issued: None,
            files: Vec::<File<'_>>::new(),
        })
        .unwrap();
    let error = builder
        .push(&Record {
            id: 2,
            title: None,
            issued: None,
            files: Unserializable,
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(
        builder
            .push(&Record {
                id: 3,
                title: None,
                issued: None,
                files: Vec::<File<'_>>::new()
            })
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        builder.finish().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    for invalid in [serde_json::json!({}), serde_json::json!({"id": null})] {
        let mut builder = RecordBatchBuilder::new(schema(), Some(vec!["id".into()])).unwrap();
        assert_eq!(
            builder.push(&invalid).unwrap_err().kind(),
            ErrorKind::Decode
        );
    }
}
