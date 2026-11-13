use arrow_schema::{ArrowError, DataType};

use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn schema_mismatch() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "read_file_set",
        "file or batch schema differs from the fixed dataset schema",
    )
}

pub(super) fn empty_selection(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        operation,
        "file selection is empty",
    )
}

fn union_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "union_by_name", message)
}

pub(super) fn arrow_error(source: ArrowError) -> Error {
    union_error(source.to_string()).with_source(source)
}

pub(super) fn duplicate_field(name: &str) -> Error {
    union_error(format!("duplicate field '{name}'"))
}

pub(super) fn schema_metadata_conflict(key: &str) -> Error {
    union_error(format!("conflicting schema metadata for key '{key}'"))
}

pub(super) fn field_metadata_conflict(name: &str, key: &str) -> Error {
    arrow_error(ArrowError::SchemaError(format!(
        "Fail to merge field '{name}' due to conflicting metadata data value for key {key}",
    )))
}

pub(super) fn field_names_mismatch(left: &str, right: &str) -> Error {
    union_error(format!("list element names differ: '{left}' and '{right}'"))
}

pub(super) fn field_union_error(name: &str, source: Error) -> Error {
    union_error(format!("field '{name}': {source}")).with_source(source)
}

pub(super) fn incompatible_types(left: &DataType, right: &DataType) -> Error {
    union_error(format!("incompatible field types: {left} and {right}"))
}

pub(super) fn array_schema_mismatch() -> Error {
    union_error("array type differs from its inspected schema")
}

pub(super) fn null_array_too_large(data_type: &DataType, len: usize) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "union_by_name",
        format!("cannot fill {len} null values for {data_type}: encoded length would overflow"),
    )
}

pub(super) fn nested_file_policy() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "plan_file_set",
        "place execution and emission layers around the complete file set",
    )
}

pub(super) fn file_ordering_mismatch() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "plan_file_set",
        "file plan violates the accessor-wide reorderable contract",
    )
}
