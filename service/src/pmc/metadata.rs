use super::{schema, validation};
use arrow_array::{ArrayRef, LargeStringArray};
use arrow_schema::Schema;
use open_arrow_access_core::{
    FieldNameIndex, RecordBatch, Result, SchemaRef, record_batch_with_rows,
};
use std::sync::{Arc, OnceLock};

/// Preserve the upstream JSON field name while exposing the URL as Arrow `text`.
fn source_metadata_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let output = schema();
            let fields = output
                .fields()
                .iter()
                .filter(|field| field.name() != "metadata_json")
                .map(|field| {
                    if field.name() == "text" {
                        Arc::new(field.as_ref().clone().with_name("text_url"))
                    } else {
                        field.clone()
                    }
                })
                .collect::<Vec<_>>();
            Arc::new(Schema::new_with_metadata(fields, output.metadata().clone()))
        })
        .clone()
}

pub(super) async fn decode_batch(
    raw: &[u8],
    path: &str,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    // Read and validate the complete metadata before applying output projection.
    let source_schema = source_metadata_schema();
    // Provider metadata permits extra upstream fields, but keeps its own field
    // types strict. Generic JsonFormat intentionally has different semantics.
    let decode_error = |source: arrow_schema::ArrowError| {
        super::error::decode(format!("{path}: {source}")).with_source(source)
    };
    let mut reader = arrow_json::ReaderBuilder::new(source_schema.clone())
        .with_strict_mode(false)
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
    let metadata_json = std::str::from_utf8(raw).map_err(super::error::metadata_utf8)?;
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "metadata_json" {
                return Arc::new(LargeStringArray::from(vec![metadata_json])) as ArrayRef;
            }
            let source_name = match field.name().as_str() {
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
