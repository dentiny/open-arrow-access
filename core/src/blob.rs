use arrow_schema::Field;

/// Shared field metadata contract understood by open-arrow-sink.
pub const BLOB_METADATA_KEY: &str = "open-arrow-sink:blob";
/// Original bytes already present in the column; no download is needed.
pub const BLOB_BYTES_VALUE: &str = "bytes";
/// A selected content URI, ingested or retained according to sink configuration.
pub const BLOB_URI_VALUE: &str = "uri";
/// Legacy type-inferred marker. New sources should declare bytes or URI explicitly.
pub const BLOB_METADATA_VALUE: &str = "true";

/// Declare an existing binary payload, preserving type, nullability and metadata.
pub fn blob_bytes_field(field: Field) -> Field {
    annotate(field, BLOB_BYTES_VALUE)
}

/// Declare a selected content URI; ordinary reference links should stay unmarked.
pub fn blob_uri_field(field: Field) -> Field {
    annotate(field, BLOB_URI_VALUE)
}

fn annotate(field: Field, kind: &str) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(BLOB_METADATA_KEY.into(), kind.into());
    field.with_metadata(metadata)
}
