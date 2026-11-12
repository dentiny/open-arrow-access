use std::collections::HashSet;

use super::inference::CsvInferenceOptions;
use arrow_schema::{DataType, Schema, TimeUnit};
use open_arrow_access_core::Result;

pub(super) fn validate_input_schema(schema: &Schema) -> Result<()> {
    if schema.fields().is_empty() {
        return Err(super::error::empty_input_schema());
    }
    Ok(())
}

// Arrow CSV reports unsupported types at flush time; reject configuration errors
// before I/O, while allowing unsupported fields omitted by projection.
pub(super) fn validate_selected_schema(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        if !supported_type(field.data_type()) {
            return Err(super::error::unsupported_field(field));
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

impl CsvInferenceOptions {
    pub(super) fn validate(self) -> Result<()> {
        if self.max_records == 0 || self.max_bytes == 0 {
            return Err(super::error::invalid_inference_limits());
        }
        Ok(())
    }
}

pub(super) fn validate_sample(sample: &[u8]) -> Result<()> {
    if sample.is_empty() {
        return Err(super::error::empty_inference_sample());
    }
    Ok(())
}

pub(super) fn validate_inferred_schema(schema: &Schema) -> Result<()> {
    let mut names = HashSet::new();
    for field in schema.fields() {
        if field.name().is_empty() || !names.insert(field.name()) {
            return Err(super::error::invalid_inferred_headers());
        }
        if field.data_type() == &DataType::Null {
            return Err(super::error::null_inferred_column());
        }
    }

    Ok(())
}

pub(super) fn require_dialect(dialect: Option<super::CsvDialect>) -> Result<super::CsvDialect> {
    dialect.ok_or_else(super::error::uninspectable_dialect)
}
