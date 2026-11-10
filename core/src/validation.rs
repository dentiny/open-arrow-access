use std::{collections::HashSet, ops::Range};

use arrow_schema::{DataType, Fields};

use crate::{Error, ErrorKind, Result};

/// Validate a half-open byte range against the current representation's length.
/// Empty ranges at or before EOF are valid. This performs no I/O and leaves
/// short-read detection to the caller; the supplied operation is kept in errors.
pub fn validate_byte_range(range: &Range<u64>, len: u64, operation: &'static str) -> Result<()> {
    if range.start > range.end || range.end > len {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            operation,
            format!(
                "range {}..{} is outside object length {}",
                range.start, range.end, len,
            ),
        ));
    }
    Ok(())
}

// These restrictions apply to RecordBatchBuilder, not to arbitrary Arrow schemas.
pub(crate) fn validate_record_batch_fields(fields: &Fields) -> Result<()> {
    let mut seen = HashSet::new();
    for field in fields {
        if !seen.insert(field.name()) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "build_record_batch",
                "duplicate schema field name",
            ));
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
        _ => Err(Error::new(
            ErrorKind::Unsupported,
            "build_record_batch",
            format!("unsupported metadata type {data_type}"),
        )),
    }
}
