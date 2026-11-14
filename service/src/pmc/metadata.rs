use super::{schema, validation};
use arrow_array::RecordBatchOptions;
use arrow_schema::Schema;
use open_arrow_access_core::{Bytes, FieldNameIndex, ReadRequest, RecordBatch, Result, SchemaRef};
use open_arrow_access_file_format::{json::JsonFormat, read_document};
use std::sync::{Arc, OnceLock};

/// Preserve the upstream JSON field name while exposing the URL as Arrow `text`.
fn json_schema() -> SchemaRef {
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
    let schema = schema();
    let batch = read_document(
        Bytes::copy_from_slice(raw),
        JsonFormat::new(json_schema()),
        ReadRequest::new(path),
    )
    .await?;
    static FIELD_INDEX: OnceLock<FieldNameIndex> = OnceLock::new();
    let field_index = FIELD_INDEX.get_or_init(|| FieldNameIndex::new(schema.fields()));
    validation::validate_metadata(&batch, path, field_index)?;
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column(
                    field_index
                        .index_of(field.name())
                        .expect("validated PMC projection"),
                )
                .clone()
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new_with_options(
        output_schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(super::error::record_batch)
}
