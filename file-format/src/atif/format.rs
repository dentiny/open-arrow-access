use super::reader::create_reader;
use atif_arrow::trajectory_schema;
use open_arrow_access_core::{
    ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest, ReaderFactory,
    RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef, async_trait,
};

/// The explicitly selected layout of ATIF documents; extensions are not inspected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtifFormat {
    /// One complete trajectory document, including pretty-printed JSON.
    Json,
    /// One complete trajectory per nonblank line. Arrays are not expanded.
    JsonLines,
}

#[async_trait]
impl ReaderFactory for AtifFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = (SchemaRef, Option<Vec<usize>>);

    fn capability(&self, input: Capability) -> RecordCapability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::PreserveGlobal)
    }

    fn prepare(&self, request: &ReadRequest) -> Result<Self::Prepared> {
        let options = &request.options;
        options.resolve_projection(&trajectory_schema())
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        (schema, projection): Self::Prepared,
    ) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(create_reader(
            input,
            *self,
            request.options.batch_size.get(),
            projection,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            /*first_index=*/ 1,
        )))
    }
}
