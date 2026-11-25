use arrow_schema::Field;

/// Shared field metadata contract understood by open-arrow-sink.
pub const BLOB_METADATA_KEY: &str = "open-arrow-sink:blob";
/// Exact value enabling native blob handling for a field.
pub const BLOB_METADATA_VALUE: &str = "true";

/// Declare an original payload or explicitly selected downloadable URL as a blob.
/// Preserves the field type, nullability and existing metadata. Readers must put
/// the same metadata on nested array fields as on their advertised schema.
/// Physical blob encoding and download policy belong to the sink.
pub fn blob_field(field: Field) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(BLOB_METADATA_KEY.into(), BLOB_METADATA_VALUE.into());
    field.with_metadata(metadata)
}
