use super::{schema, validation};
use arrow_schema::Schema;
use futures::stream;
use open_arrow_access_core::{
    Bytes, FieldNameIndex, OrderRequirement, ReadRequest, RecordBatch, RecordMetadata, Result,
    SchemaRef, StreamReader, project_batch,
};
use open_arrow_access_file_format::{json::JsonFormat, read_document};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};
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

/// Reuse the record layer for the output rename while keeping source JSON names.
fn text_column_layer() -> &'static SchemaEvolutionLayer {
    static LAYER: OnceLock<SchemaEvolutionLayer> = OnceLock::new();
    LAYER.get_or_init(|| {
        SchemaEvolutionLayer::try_new(vec![ColumnEvolution::rename_column("text_url", "text")])
            .expect("valid PMC text column rename")
    })
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
        JsonFormat::new(source_metadata_schema()),
        ReadRequest::new(path),
    )
    .await?;
    static FIELD_INDEX: OnceLock<FieldNameIndex> = OnceLock::new();
    let field_index = FIELD_INDEX.get_or_init(|| FieldNameIndex::new(schema.fields()));
    validation::validate_metadata(&batch, path, field_index)?;
    let input = StreamReader::records(
        stream::once(async move { Ok(batch) }),
        RecordMetadata {
            schema: source_metadata_schema(),
            source: Some(path.into()),
        },
        OrderRequirement::Reorderable,
    );
    let mut reader = text_column_layer().wrap_reader(Box::new(input))?;
    let batch = reader.next().await?.expect("one decoded PMC metadata row");
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
