use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{ErrorKind, FieldNameIndex, ReadOptions};

#[test]
fn owns_names_and_matches_first_duplicate_without_scanning() {
    let index = {
        let schema = Schema::new(vec![
            Field::new("same", DataType::UInt64, false),
            Field::new("same", DataType::Utf8, false),
            Field::new("other", DataType::Utf8, true),
        ]);
        FieldNameIndex::new(schema.fields())
    };
    assert_eq!(index.get("same"), Some(0));
    assert_eq!(index.index_of("other").unwrap(), 2);
    assert_eq!(index.get("missing"), None);
    assert!(index.index_of("missing").is_err());
    assert_eq!(
        FieldNameIndex::new(Schema::empty().fields()).get("same"),
        None
    );
}

#[test]
fn wide_projections_keep_requested_order_metadata_and_errors() {
    let count = 2048;
    let schema = Arc::new(Schema::new_with_metadata(
        (0..count)
            .map(|i| Field::new(format!("c{i}"), DataType::UInt64, true))
            .collect::<Vec<_>>(),
        HashMap::from([("snapshot".into(), "wide".into())]),
    ));
    let columns = (0..count)
        .rev()
        .map(|i| format!("c{i}"))
        .collect::<Vec<_>>();
    let (projected, indices) = ReadOptions {
        projection: Some(columns.clone()),
        ..Default::default()
    }
    .resolve_projection(&schema)
    .unwrap();
    assert_eq!(indices.unwrap(), (0..count).rev().collect::<Vec<_>>());
    assert_eq!(projected.metadata(), schema.metadata());
    assert_eq!(
        projected
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        columns
    );
    for columns in [vec!["c0".into(), "c0".into()], vec!["missing".into()]] {
        let error = ReadOptions {
            projection: Some(columns),
            ..Default::default()
        }
        .resolve_projection(&schema)
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    let (empty, indices) = ReadOptions {
        projection: Some(vec![]),
        ..Default::default()
    }
    .resolve_projection(&schema)
    .unwrap();
    assert!(empty.fields().is_empty());
    assert_eq!(empty.metadata(), schema.metadata());
    assert_eq!(indices, Some(vec![]));
}
