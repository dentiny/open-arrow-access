use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::{
    validate_evolution_batch_schema, validate_evolution_source, validate_evolution_target,
};

#[test]
fn target_checks_use_the_current_slot_without_scanning_fields() {
    for (source, target) in [(None, None), (Some(0), None), (Some(0), Some(0))] {
        validate_evolution_target(source, target).unwrap();
    }
    for (source, target) in [(None, Some(0)), (Some(0), Some(1))] {
        assert!(validate_evolution_target(source, target).is_err());
    }
}

#[test]
fn source_uniqueness_uses_index_size() {
    validate_evolution_source(4096, 4096).unwrap();
    validate_evolution_source(0, 0).unwrap();
    assert!(validate_evolution_source(1, 2).is_err());
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
