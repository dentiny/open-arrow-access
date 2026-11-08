use open_arrow_access_core::{
    DefaultDocumentAccessor, DocumentDecoder, ReadRequest, RecordBatch, Result, SchemaRef,
    async_trait,
};

use super::{metadata, schema};

/// Read one article-version JSON object; content URLs remain metadata.
pub type PmcAccessor<A> = DefaultDocumentAccessor<A, PmcDecoder>;

/// Convert one PMC metadata JSON document into its projected article row.
/// Use with [`open_arrow_access_core::DocumentAccessor`] for explicit composition.
#[derive(Default)]
pub struct PmcDecoder;

#[async_trait]
impl DocumentDecoder for PmcDecoder {
    type Request = ReadRequest;
    fn prepare(&self, _: &ReadRequest) -> Result<SchemaRef> {
        Ok(schema())
    }
    async fn decode(
        &self,
        bytes: &[u8],
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        metadata::decode_batch(bytes, request.object.locator(), schema)
    }
}
