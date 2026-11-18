use super::{PmcFormat, schema, validation};
use arrow_array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Schema};
use open_arrow_access_core::{
    FieldNameIndex, RecordBatch, Result, SchemaRef, record_batch_with_rows,
};
use std::sync::{Arc, OnceLock};

/// Input fields use the original names retained by the metadata struct.
fn source_metadata_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let output = schema();
            let DataType::Struct(fields) = output
                .field_with_name("metadata")
                .expect("PMC metadata field")
                .data_type()
            else {
                unreachable!("PMC metadata is a struct");
            };
            Arc::new(Schema::new(fields.clone()))
        })
        .clone()
}

pub(super) async fn decode_batch(
    raw: &[u8],
    path: &str,
    output_schema: SchemaRef,
    format: PmcFormat,
) -> Result<RecordBatch> {
    // Read and validate the complete metadata before applying output projection.
    let source_schema = source_metadata_schema();
    // Reject unrecognized fields instead of silently dropping upstream metadata.
    let decode_error = |source: arrow_schema::ArrowError| {
        super::error::decode(format!("{path}: {source}")).with_source(source)
    };
    let mut reader = arrow_json::ReaderBuilder::new(source_schema.clone())
        .with_strict_mode(true)
        .with_coerce_primitive(false)
        .with_batch_size(2)
        .build(raw)
        .map_err(decode_error)?;
    let batch = reader
        .next()
        .transpose()
        .map_err(decode_error)?
        .ok_or_else(|| super::error::decode(format!("{path}: expected one metadata object")))?;
    if batch.num_rows() != 1 || reader.next().transpose().map_err(decode_error)?.is_some() {
        return Err(super::error::decode(format!(
            "{path}: expected one metadata object"
        )));
    }
    static FIELD_INDEX: OnceLock<FieldNameIndex> = OnceLock::new();
    let field_index = FIELD_INDEX.get_or_init(|| FieldNameIndex::new(source_schema.fields()));
    validation::validate_metadata(&batch, path, field_index)?;
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "metadata" {
                return Arc::new(StructArray::from(batch.clone())) as ArrayRef;
            }
            let source_name = match field.name().as_str() {
                "content_url" => format.metadata_field(),
                "text" => "text_url",
                name => name,
            };
            let index = field_index
                .index_of(source_name)
                .expect("validated PMC projection");
            batch.column(index).clone()
        })
        .collect();
    record_batch_with_rows(output_schema, columns, batch.num_rows())
        .map_err(|error| super::error::decode(error.to_string()).with_source(error))
}
