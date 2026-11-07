use super::{
    PmcLimits, capability, decode,
    input::{JsonInput, read_bounded},
    metadata, schema,
};
use arrow_array::{ArrayRef, LargeStringArray};
use arrow_schema::Schema;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ReadRequest, Reader, RecordBatch,
    RecordMetadata, Result, async_trait,
};
use open_arrow_access_file_format::JsonAccessor;
use std::sync::Arc;

pub(super) struct PmcReader<A> {
    pub accessor: Arc<A>,
    pub request: Option<ReadRequest>,
    pub limits: PmcLimits,
    pub projection: Option<Vec<usize>>,
    pub metadata: RecordMetadata,
}
impl<A> PmcReader<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn read_article(&self, request: ReadRequest) -> Result<RecordBatch> {
        let raw = read_bounded(
            self.accessor.as_ref(),
            request.object.clone(),
            request.byte_options,
            self.limits.metadata_bytes.get(),
        )
        .await?;
        let mapped = metadata::parse(&raw, request.object.locator())?;
        let normalized = serde_json::to_vec(&mapped)
            .map_err(|error| decode(error.to_string()).with_source(error))?;
        let full_schema = schema();
        let fields = full_schema.fields();
        let json_schema = Arc::new(Schema::new(fields[..fields.len() - 1].to_vec()));
        let mut reader = JsonAccessor::new(JsonInput(normalized.into()), json_schema)
            .open(ReadRequest::new("pmc-metadata"))
            .await?;
        let batch = reader
            .next()
            .await?
            .ok_or_else(|| decode("missing metadata row"))?;
        let raw_text = std::str::from_utf8(&raw)
            .map_err(|error| decode(error.to_string()).with_source(error))?;
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(LargeStringArray::from(vec![raw_text])) as ArrayRef);
        let batch = RecordBatch::try_new(full_schema, columns)
            .map_err(|error| decode(error.to_string()).with_source(error))?;
        match &self.projection {
            Some(indices) => batch
                .project(indices)
                .map_err(|error| decode(error.to_string()).with_source(error)),
            None => Ok(batch),
        }
    }
}
#[async_trait]
impl<A> Reader for PmcReader<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    fn capability(&self) -> Capability {
        capability(true)
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        let Some(request) = self.request.take() else {
            return Ok(None);
        };
        self.read_article(request).await.map(Some)
    }
}
