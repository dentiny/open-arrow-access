use arrow_schema::{DataType, Schema, TimeUnit};
use open_arrow_access_core::{Error, ErrorKind, Result};

pub(super) fn validate_input_schema(schema: &Schema) -> Result<()> {
    if schema.fields().is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "open_csv",
            "CSV requires an input schema with at least one field",
        ));
    }
    Ok(())
}

// Arrow CSV reports unsupported types at flush time; reject configuration errors
// before I/O, while allowing unsupported fields omitted by projection.
pub(super) fn validate_selected_schema(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        if !supported_type(field.data_type()) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "open_csv",
                format!(
                    "CSV does not support field {:?} of type {:?}",
                    field.name(),
                    field.data_type()
                ),
            ));
        }
    }
    Ok(())
}

fn supported_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(_, _)
        | DataType::Null
        | DataType::Utf8
        | DataType::Utf8View => true,
        DataType::Dictionary(key, value) => {
            value.as_ref() == &DataType::Utf8
                && matches!(
                    key.as_ref(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                )
        }
        _ => false,
    }
}
