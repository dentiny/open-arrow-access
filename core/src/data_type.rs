use arrow_schema::DataType;

/// Format-independent families of Arrow types, preserving the outer type of
/// containers and encodings. Widths, units, precision and children stay available
/// on the original [`DataType`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum DataTypeCategory {
    Null,
    Boolean,
    Integer,
    Float,
    Decimal,
    Date,
    Time,
    Timestamp,
    Duration,
    Interval,
    String,
    Binary,
    List,
    Struct,
    Union,
    Dictionary,
    Map,
    RunEndEncoded,
}

/// Classify any Arrow type without choosing a file format or validating type
/// parameters. Containers are classified by their outer type, not their child.
/// Format support is a separate decision using the category and original type.
///
/// [`DataType`] also supplies narrower predicates such as `is_integer`,
/// `is_floating`, `is_numeric`, `is_temporal`, `is_string` and `is_nested`.
///
/// ```
/// use open_arrow_access_core::{DataType, DataTypeCategory, classify_data_type};
///
/// assert_eq!(classify_data_type(&DataType::UInt16), DataTypeCategory::Integer);
/// assert_eq!(classify_data_type(&DataType::LargeUtf8), DataTypeCategory::String);
/// let encoded = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
/// assert_eq!(classify_data_type(&encoded), DataTypeCategory::Dictionary);
/// ```
pub fn classify_data_type(data_type: &DataType) -> DataTypeCategory {
    use DataTypeCategory as Category;
    match data_type {
        DataType::Null => Category::Null,
        DataType::Boolean => Category::Boolean,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Category::Integer,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Category::Float,
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Category::Decimal,
        DataType::Date32 | DataType::Date64 => Category::Date,
        DataType::Time32(_) | DataType::Time64(_) => Category::Time,
        DataType::Timestamp(_, _) => Category::Timestamp,
        DataType::Duration(_) => Category::Duration,
        DataType::Interval(_) => Category::Interval,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Category::String,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Category::Binary,
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => Category::List,
        DataType::Struct(_) => Category::Struct,
        DataType::Union(_, _) => Category::Union,
        DataType::Dictionary(_, _) => Category::Dictionary,
        DataType::Map(_, _) => Category::Map,
        DataType::RunEndEncoded(_, _) => Category::RunEndEncoded,
    }
}
