use super::{schema, validation};
use arrow_array::RecordBatchOptions;
use arrow_schema::Schema;
use open_arrow_access_core::{Bytes, FieldNameIndex, ReadRequest, RecordBatch, Result, SchemaRef};
use open_arrow_access_file_format::{json::JsonFormat, read_document};
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
    let batch = read_document(
        Bytes::copy_from_slice(raw),
        JsonFormat::new(source_schema.clone()),
        ReadRequest::new(path),
    )
    .await?;
    static FIELD_INDEX: OnceLock<FieldNameIndex> = OnceLock::new();
    let field_index = FIELD_INDEX.get_or_init(|| FieldNameIndex::new(source_schema.fields()));
    validation::validate_metadata(&batch, path, field_index)?;
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
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
    RecordBatch::try_new_with_options(
        output_schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|error| super::error::decode(error.to_string()).with_source(error))
}
