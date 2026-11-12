use std::{collections::HashSet, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::FieldNameIndex;

use super::{validate_evolution_batch_schema, validate_evolution_names, validate_evolution_source};
use crate::ColumnEvolution;

#[test]
fn names_check_only_change_targets_against_the_existing_index() {
    let schema = Schema::new(
        (0..4096)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    );
    let names = FieldNameIndex::new(schema.fields());
    validate_evolution_source(&names).unwrap();
    let changes = vec![
        ColumnEvolution::rename_column("c0", "c1"),
        ColumnEvolution::remove_column("c1"),
        ColumnEvolution::add_column("c0", DataType::Int32, |_| unreachable!()),
    ];
    validate_evolution_names(&changes, &names, &HashSet::from([1])).unwrap();
    let collision = [ColumnEvolution::add_column(
        "c4095",
        DataType::Int32,
        |_| unreachable!(),
    )];
    assert!(validate_evolution_names(&collision, &names, &HashSet::new()).is_err());
}

#[test]
fn source_uniqueness_uses_index_metadata_and_preserves_first_lookup() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("a", DataType::Utf8, true),
    ]);
    let names = FieldNameIndex::new(schema.fields());
    assert_eq!(names.get("a"), Some(0));
    assert!(validate_evolution_source(&names).is_err());
}

#[test]
fn batch_checks_width_and_affected_fields_without_comparing_the_whole_schema() {
    let expected = Arc::new(Schema::new(vec![
        Field::new("changed", DataType::Int32, true),
        Field::new("untouched", DataType::Int32, true),
    ]));
    let actual = Arc::new(Schema::new(vec![
        expected.field(0).clone(),
        Field::new("not_inspected", DataType::Utf8, true),
    ]));
    validate_evolution_batch_schema(&actual, &expected, &[0]).unwrap();
    assert!(validate_evolution_batch_schema(&actual, &expected, &[1]).is_err());
    assert!(validate_evolution_batch_schema(&Arc::new(Schema::empty()), &expected, &[]).is_err());
}
