use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Field, Fields, UnionMode};
use open_arrow_access_core::{EmissionPolicy, OrderRequirement, Result, ScanPlan};

use super::error;

// Execution policies belong to the enclosing dataset scan. Flattening a child
// policy would otherwise silently discard settings or introduce another budget.
pub(super) fn validate_file_policy(plan: &ScanPlan) -> Result<()> {
    if plan.parallel_read.is_some() || plan.emission != EmissionPolicy::Ordered {
        return Err(error::nested_file_policy());
    }
    Ok(())
}

pub(super) fn validate_file_ordering(
    ordering: OrderRequirement,
    file_ordering: OrderRequirement,
) -> Result<()> {
    if ordering == OrderRequirement::Reorderable && file_ordering != OrderRequirement::Reorderable {
        return Err(error::file_ordering_mismatch());
    }
    Ok(())
}

pub(super) fn validate_fields(fields: &Fields) -> Result<()> {
    let mut names = HashSet::new();
    for field in fields {
        if !names.insert(field.name()) {
            return Err(error::duplicate_field(field.name()));
        }
        match field.data_type() {
            DataType::Struct(fields) => validate_fields(fields)?,
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::Map(item, _) => {
                validate_fields(&vec![item.clone()].into())?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(super) fn validate_schema_metadata(
    left: &HashMap<String, String>,
    right: &HashMap<String, String>,
) -> Result<()> {
    for (key, value) in right {
        if left.get(key).is_some_and(|existing| existing != value) {
            return Err(error::schema_metadata_conflict(key));
        }
    }
    Ok(())
}

pub(super) fn validate_field_names(left: &Field, right: &Field) -> Result<()> {
    if left.name() != right.name() {
        return Err(error::field_names_mismatch(left.name(), right.name()));
    }
    Ok(())
}

pub(super) fn validate_schema(
    actual: &arrow_schema::SchemaRef,
    expected: &arrow_schema::SchemaRef,
) -> Result<()> {
    if actual != expected {
        return Err(super::error::schema_mismatch());
    }
    Ok(())
}

pub(super) fn validate_null_array(data_type: &DataType, len: usize) -> Result<()> {
    if len == 0 {
        return Ok(());
    }
    match data_type {
        DataType::RunEndEncoded(run_ends, values) => {
            let fits = match run_ends.data_type() {
                DataType::Int16 => i16::try_from(len).is_ok(),
                DataType::Int32 => i32::try_from(len).is_ok(),
                DataType::Int64 => i64::try_from(len).is_ok(),
                _ => false,
            };
            if !fits {
                return Err(error::null_array_too_large(data_type, len));
            }
            validate_null_array(values.data_type(), 1)?;
        }
        DataType::Struct(fields) => {
            for field in fields {
                validate_null_array(field.data_type(), len)?;
            }
        }
        DataType::FixedSizeList(item, size) => {
            let child_len = usize::try_from(*size)
                .ok()
                .and_then(|size| len.checked_mul(size))
                .ok_or_else(|| error::null_array_too_large(data_type, len))?;
            validate_null_array(item.data_type(), child_len)?;
        }
        DataType::Union(fields, mode) => {
            // Arrow fills every sparse child, but only the first dense child.
            for (index, (_, field)) in fields.iter().enumerate() {
                if index == 0 || *mode == UnionMode::Sparse {
                    validate_null_array(field.data_type(), len)?;
                }
            }
        }
        // Variable-size lists, maps and dictionaries construct empty children.
        _ => {}
    }
    Ok(())
}
