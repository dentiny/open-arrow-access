use super::{schema, validation};
use open_arrow_access_core::{
    Bytes, FieldNameIndex, ReadRequest, RecordBatch, Result, SchemaRef, project_batch,
};
use open_arrow_access_file_format::{json::JsonFormat, read_document};
use std::sync::OnceLock;

pub(super) async fn decode_batch(
    raw: &[u8],
    path: &str,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    // Read and validate the complete metadata before applying output projection.
    let schema = schema();
    let batch = read_document(
        Bytes::copy_from_slice(raw),
        JsonFormat::new(schema.clone()),
        ReadRequest::new(path),
    )
    .await?;
    validation::validate_metadata(&batch, path)?;
    static FIELD_INDEX: OnceLock<FieldNameIndex> = OnceLock::new();
    let field_index = FIELD_INDEX.get_or_init(|| FieldNameIndex::new(schema.fields()));
    let projection = output_schema
        .fields()
        .iter()
        .map(|field| {
            field_index
                .index_of(field.name())
                .expect("validated PMC projection")
        })
        .collect::<Vec<_>>();
    project_batch(batch, Some(&projection), output_schema)
}
