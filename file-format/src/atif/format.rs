use super::reader::create_reader;
use atif_arrow::trajectory_schema;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, FactoryToken, OrderRequirement, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, RecordReader, ResolvedProjection, Result,
    async_trait,
};

/// ATIF document framing; extensions are not inspected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AtifFormat {
    /// One complete trajectory document, including pretty-printed JSON.
    Json,
    /// One complete trajectory per nonblank line. Arrays are not expanded.
    JsonLines,
    /// Frame JSON objects from their contents, accepting pretty JSON and JSONL.
    /// Inter-document whitespace is omitted from each row's raw_json.
    #[default]
    Auto,
}

#[async_trait]
impl ReaderFactory for AtifFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = ResolvedProjection;

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

    async fn metadata(
        &self,
        _storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: Self::Request,
    ) -> Result<RecordMetadata> {
        Ok(RecordMetadata {
            schema: self.prepare(&request)?.schema,
            source: Some(request.object),
        })
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        ResolvedProjection {
            schema,
            indices: projection,
        }: Self::Prepared,
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
