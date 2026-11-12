use super::{schema, validation};
use arrow_schema::Schema;
use open_arrow_access_core::{
    Bytes, FieldNameIndex, ReadRequest, RecordBatch, Result, SchemaRef, project_batch,
};
use open_arrow_access_file_format::{json::JsonFormat, read_document};
use std::sync::{Arc, OnceLock};

pub(super) async fn decode_batch(
    raw: &[u8],
    path: &str,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    // Read and validate the complete metadata before applying output projection.
    let schema = schema();
    // Upstream JSON keeps pdf_url; only the public Arrow field is named pdf.
    let input_schema = Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "pdf" {
                    Arc::new(field.as_ref().clone().with_name("pdf_url"))
                } else {
                    field.clone()
                }
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ));
    let batch = read_document(
        Bytes::copy_from_slice(raw),
        JsonFormat::new(input_schema),
        ReadRequest::new(path),
    )
    .await?;
    validation::validate_metadata(&batch, path)?;
    let batch = RecordBatch::try_new(schema.clone(), batch.into_parts().1)
        .map_err(|error| super::error::decode(error.to_string()).with_source(error))?;
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
