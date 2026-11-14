use std::{collections::HashSet, ops::Range};

use arrow_schema::{DataType, Fields};

use crate::{Result, SchemaRef};

/// Validate a half-open byte range against the current representation's length.
/// Empty ranges at or before EOF are valid. This performs no I/O and leaves
/// short-read detection to the caller; the supplied operation is kept in errors.
pub fn validate_byte_range(range: &Range<u64>, len: u64, operation: &'static str) -> Result<()> {
    if range.start > range.end || range.end > len {
        return Err(crate::error::invalid_byte_range(range, len, operation));
    }
    Ok(())
}

// These restrictions apply to RecordBatchBuilder, not to arbitrary Arrow schemas.
pub(crate) fn validate_record_batch_fields(fields: &Fields) -> Result<()> {
    let mut seen = HashSet::new();
    for field in fields {
        if !seen.insert(field.name()) {
            return Err(crate::error::duplicate_schema_field());
        }
        validate_record_batch_type(field.data_type())?;
    }
    Ok(())
}

fn validate_record_batch_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int32
        | DataType::Date32
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => Ok(()),
        DataType::Struct(fields) => validate_record_batch_fields(fields),
        DataType::List(field) | DataType::LargeList(field) => {
            validate_record_batch_type(field.data_type())
        }
        _ => Err(crate::error::unsupported_metadata_type(data_type)),
    }
}

pub(crate) fn validate_scan_schema(
    actual: &arrow_schema::SchemaRef,
    expected: &arrow_schema::SchemaRef,
    schema_error: fn() -> crate::Error,
) -> Result<()> {
    if actual != expected {
        return Err(schema_error());
    }
    Ok(())
}

pub(crate) fn validate_document_batch(
    batch: &crate::RecordBatch,
    schema: &SchemaRef,
) -> Result<()> {
    if batch.num_rows() != 1 || batch.schema() != *schema {
        return Err(crate::error::invalid_document_batch());
    }
    Ok(())
}

pub(crate) fn validate_read_capability(supported: bool, operation: &'static str) -> Result<()> {
    if !supported {
        return Err(crate::error::reading_unsupported(operation));
    }
    Ok(())
}

pub(crate) fn validate_listing_capability(supported: bool) -> Result<()> {
    if !supported {
        return Err(crate::error::listing_unsupported());
    }
    Ok(())
}

pub(crate) fn validate_seek_input(
    reader: &dyn crate::ByteReader,
    operation: &'static str,
) -> Result<u64> {
    if !reader.capability().read_with_range {
        return Err(crate::error::seek_requires_ranges(operation));
    }
    reader
        .metadata()
        .content_length
        .ok_or_else(|| crate::error::seek_requires_length(operation))
}

pub(crate) fn validate_range_length(
    actual: u64,
    expected: u64,
    operation: &'static str,
) -> Result<()> {
    if actual != expected {
        return Err(crate::error::incorrect_range_length(operation));
    }
    Ok(())
}
