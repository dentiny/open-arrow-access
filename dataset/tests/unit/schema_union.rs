use std::collections::HashMap;

use super::*;

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

fn structure(names: &[&str]) -> DataType {
    DataType::Struct(
        names
            .iter()
            .map(|name| field(name, DataType::Int64))
            .collect(),
    )
}

fn merge(schemas: impl IntoIterator<Item = Schema>) -> Schema {
    let mut union = SchemaUnion::default();
    for schema in schemas {
        union.merge(&schema).unwrap();
    }
    union.finish()
}

#[test]
fn growing_sparse_schemas_preserve_order_and_required_fields() {
    let schema = merge((0..256).map(|index| {
        Schema::new(vec![
            field("always", DataType::Int64),
            field(&format!("column_{index}"), DataType::Int64),
            field("nested", structure(&[&format!("child_{index}")])),
        ])
    }));
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.fields().len(), 258);
    assert_eq!(schema.field(1).name(), "column_0");
    assert_eq!(schema.field(2).name(), "nested");
    assert_eq!(schema.field(257).name(), "column_255");
    let DataType::Struct(children) = schema.field(2).data_type() else {
        panic!("expected struct");
    };
    for (index, child) in children.iter().enumerate() {
        assert_eq!(child.name(), &format!("child_{index}"));
        assert!(child.is_nullable());
    }
    assert!(
        schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("column_"))
            .all(|field| field.is_nullable())
    );
}

#[test]
fn missing_and_null_parents_do_not_make_present_children_nullable() {
    let present = Schema::new(vec![field("parent", structure(&["required"]))]);
    let absent = Schema::empty();
    let null = Schema::new(vec![field("parent", DataType::Null)]);
    for schemas in [
        vec![absent.clone(), present.clone(), null.clone()],
        vec![null, present, absent],
    ] {
        let schema = merge(schemas);
        assert!(schema.field(0).is_nullable());
        let DataType::Struct(children) = schema.field(0).data_type() else {
            panic!("expected struct");
        };
        assert!(!children[0].is_nullable());
    }
}

#[test]
fn list_variants_track_missing_children_when_they_reappear() {
    for kind in [
        ListKind::List,
        ListKind::LargeList,
        ListKind::ListView,
        ListKind::LargeListView,
        ListKind::FixedSizeList(2),
    ] {
        let schema = merge(
            [["x"].as_slice(), ["y"].as_slice(), ["x", "y"].as_slice()].map(|names| {
                Schema::new(vec![field(
                    "items",
                    kind.finish(field("item", structure(names))),
                )])
            }),
        );
        let (_, item) = ListKind::from_type(schema.field(0).data_type()).unwrap();
        assert!(!item.is_nullable());
        let DataType::Struct(children) = item.data_type() else {
            panic!("expected struct");
        };
        assert_eq!(
            children
                .iter()
                .map(|field| (field.name().as_str(), field.is_nullable()))
                .collect::<Vec<_>>(),
            [("x", true), ("y", true)]
        );
    }
}

#[test]
fn metadata_and_dictionary_flags_keep_their_merge_rules() {
    let metadata = |value: &str| HashMap::from([("key".into(), value.into())]);
    let dictionary = || {
        field(
            "value",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        )
    };
    let mut union = SchemaUnion::default();
    union
        .merge(&Schema::new_with_metadata(
            vec![dictionary().with_metadata(metadata("field"))],
            metadata("schema"),
        ))
        .unwrap();
    union
        .merge(&Schema::new_with_metadata(
            vec![dictionary()],
            HashMap::from([("extra".into(), "extra".into())]),
        ))
        .unwrap();
    let schema = union.finish();
    assert_eq!(schema.metadata()["key"], "schema");
    assert_eq!(schema.metadata()["extra"], "extra");
    assert_eq!(schema.field(0).metadata()["key"], "field");
    assert!(!schema.field(0).is_nullable());
    for (left, right) in [
        (
            Schema::new_with_metadata(Vec::<Field>::new(), metadata("left")),
            Schema::new_with_metadata(Vec::<Field>::new(), metadata("right")),
        ),
        (
            Schema::new(vec![dictionary().with_metadata(metadata("left"))]),
            Schema::new(vec![dictionary().with_metadata(metadata("right"))]),
        ),
        (
            Schema::new(vec![dictionary()]),
            Schema::new(vec![dictionary().with_dict_is_ordered(true)]),
        ),
    ] {
        let mut union = SchemaUnion::default();
        union.merge(&left).unwrap();
        assert!(union.merge(&right).is_err());
    }
}
