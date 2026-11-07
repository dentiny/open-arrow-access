use arrow_schema::{DataType, TimeUnit};

/// Whether the CSV decoder supports this Arrow data type category.
///
/// This is the same check used by [`crate::CsvFileFormat`] before opening input.
/// It includes the supported integer-keyed UTF-8 dictionaries. Type parameters,
/// parser options and individual CSV values can still cause decoding to fail.
/// Other file formats have their own type support.
pub fn csv_supports_type(data_type: &DataType) -> bool {
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
