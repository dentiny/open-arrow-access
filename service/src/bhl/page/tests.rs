use super::*;
use arrow_array::{Array, new_null_array};
use arrow_schema::{DataType, Schema};
use open_arrow_access_core::RecordBatchBuilder;

pub(in crate::bhl) fn metadata(title: &str, rights: &str) -> StructArray {
    let field = page_schema().field_with_name("metadata").unwrap().clone();
    let DataType::Struct(tables) = field.data_type() else {
        panic!("expected struct")
    };
    let mut builder =
        RecordBatchBuilder::new(Arc::new(Schema::new(vec![field.clone()])), None).unwrap();
    builder
        .push(|row| {
            row.append_struct("metadata", |row| {
                for table in tables {
                    if let DataType::Struct(fields) = table.data_type() {
                        row.append_struct(table.name(), |row| {
                            for field in fields {
                                let value = match (table.name().as_str(), field.name().as_str()) {
                                    ("title", "FullTitle") => title,
                                    ("item", "RightsStatement") => rights,
                                    _ => "",
                                };
                                row.append(field.name(), value)?;
                            }
                            Ok(())
                        })?;
                    } else {
                        row.append_structs(table.name(), std::iter::empty::<()>(), |_, ()| Ok(()))?;
                    }
                }
                Ok(())
            })
        })
        .unwrap();
    builder
        .finish()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
}
fn record(metadata: StructArray) -> BhlPageRecord<'static> {
    BhlPageRecord {
        entity_type: "item",
        entity_id: 1,
        page_id: 3,
        sequence: 2,
        data: b"\r\n\xff",
        source_path: "s3://bhl-open-data/ocr/item-1/item-1-3-2.txt",
        snapshot_id: "source-snapshot",
        source_entry_index: None,
        metadata,
    }
}
#[test]
fn assigns_actual_metadata_and_original_payload() {
    for (title, rights) in [
        ("First title", "Original rights"),
        ("Second title", "Different rights"),
    ] {
        let input = metadata(title, rights);
        let batch = record(input.clone()).into_batch().unwrap();
        let actual = batch
            .column_by_name("metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(actual, &input);
        assert!(!actual.is_null(0));
        let title_array = actual
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            title_array
                .column_by_name("FullTitle")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(0),
            title
        );
        assert_eq!(
            batch
                .column_by_name("data")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            b"\r\n\xff"
        );
    }
}
#[test]
fn rejects_null_wrong_shape_or_wrong_length_metadata() {
    let schema = page_schema();
    let field = schema.field_with_name("metadata").unwrap();
    assert!(!field.is_nullable());
    for len in [0, 1, 2] {
        let array = new_null_array(field.data_type(), len);
        assert!(
            record(
                array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .clone()
            )
            .into_batch()
            .is_err()
        );
    }
    let wrong = StructArray::new_empty_fields(1, None);
    assert!(record(wrong).into_batch().is_err());
    let DataType::Struct(tables) = field.data_type() else {
        panic!("expected struct")
    };
    assert_eq!(tables.len(), 13);
    let count = tables
        .iter()
        .map(|table| {
            let data_type = match table.data_type() {
                DataType::List(element) => element.data_type(),
                data_type => data_type,
            };
            let DataType::Struct(fields) = data_type else {
                panic!("expected table")
            };
            fields.len()
        })
        .sum::<usize>();
    assert_eq!(count, 104);
}
