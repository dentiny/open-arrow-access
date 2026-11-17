use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{ErrorKind, FieldNameIndex, ReadOptions, ResolvedProjection};

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
    let ResolvedProjection {
        schema: projected,
        indices,
    } = ReadOptions::default()
        .with_projection(columns.clone())
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
        let error = ResolvedProjection::resolve(&schema, Some(&columns)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    let ResolvedProjection {
        schema: empty,
        indices,
    } = ReadOptions::default()
        .with_projection([] as [&str; 0])
        .resolve_projection(&schema)
        .unwrap();
    assert!(empty.fields().is_empty());
    assert_eq!(empty.metadata(), schema.metadata());
    assert_eq!(indices, Some(vec![]));
}

#[test]
fn request_builders_retain_other_settings_and_replace_projection() {
    use open_arrow_access_core::{ByteReadOptions, ReadRequest};
    use std::num::NonZeroUsize;

    let byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: Some(NonZeroUsize::new(17).unwrap()),
    };
    let rows = NonZeroUsize::new(5).unwrap();
    let request = ReadRequest::new("input")
        .with_byte_options(byte_options)
        .with_projection(["old"])
        .with_batch_size(rows)
        .with_projection([String::from("second"), String::from("first")]);
    assert_eq!(request.object.locator(), "input");
    assert_eq!(request.byte_options, byte_options);
    assert_eq!(request.options.batch_size, rows);
    assert_eq!(request.options.projection.unwrap(), ["second", "first"]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "first",
        DataType::Int64,
        true,
    )]));
    let options = ReadOptions::default().with_batch_size(rows);
    let full = options.resolve_projection(&schema).unwrap();
    assert!(Arc::ptr_eq(&full.schema, &schema));
    assert_eq!(full.indices, None);
    assert_eq!(options.batch_size, rows);
}
