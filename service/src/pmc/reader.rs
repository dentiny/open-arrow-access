use super::{PmcLimits, capability, decode, input::read_bounded, metadata};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, ReadRequest, Reader, RecordBatch,
    RecordMetadata, Result, async_trait,
};
use std::sync::Arc;

pub(super) struct PmcReader<A> {
    pub accessor: Arc<A>,
    pub request: Option<ReadRequest>,
    pub limits: PmcLimits,
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
        let raw_text = std::str::from_utf8(&raw)
            .map_err(|error| decode(error.to_string()).with_source(error))?;
        mapped.into_batch(raw_text, self.metadata.schema.clone())
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
