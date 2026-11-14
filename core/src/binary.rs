use std::sync::Arc;

use arrow_schema::{DataType, Field};

use crate::{
    Capability, DocumentDecoder, OrderRequirement, ReadRequest, RecordBatch, RecordBatchBuilder,
    RecordCapability, Result, Schema, SchemaRef, async_trait,
};

/// Preserve one whole object as one non-null `data: LargeBinary` value.
///
/// Compose with `DocumentAccessor::new(storage, BinaryDecoder)`. Bytes are kept
/// unchanged, including empty inputs and invalid UTF-8. The accessor buffers the
/// entire object; it does not infer formats or decompress content. An empty
/// projection still returns one row, and the input is fully read for errors.
#[derive(Debug, Default)]
pub struct BinaryDecoder;

#[async_trait]
impl DocumentDecoder for BinaryDecoder {
    type Request = ReadRequest;

    fn capability(&self, input: Capability) -> RecordCapability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    fn prepare(&self, _: &ReadRequest) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::LargeBinary,
            false,
        )])))
    }

    async fn decode(
        &self,
        bytes: &[u8],
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        RecordBatchBuilder::build_row(self.prepare(&request)?, schema, |row| {
            row.append("data", bytes)
        })
    }
}
