use arrow_schema::TimeUnit;
use open_arrow_access_core::{DataType, DataTypeCategory as Category, classify_data_type};

// Categories are shared by all formats; these additional constraints belong to CSV.
pub(super) fn supported_type(data_type: &DataType) -> bool {
    match classify_data_type(data_type) {
        Category::Null
        | Category::Boolean
        | Category::Integer
        | Category::Float
        | Category::Decimal
        | Category::Date
        | Category::Timestamp => true,
        Category::Time => matches!(
            data_type,
            DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
                | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        ),
        Category::String => matches!(data_type, DataType::Utf8 | DataType::Utf8View),
        Category::Dictionary => matches!(
            data_type,
            DataType::Dictionary(key, value)
                if key.is_dictionary_key_type() && value.as_ref() == &DataType::Utf8
        ),
        _ => false,
    }
}
