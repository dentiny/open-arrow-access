use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, FactoryAccessor,
    ReadRequest, ReaderFactory, RecordReader, Result, ScanPlan, ScanPlanner, StreamReader,
    async_trait,
};

/// An already resolved API page bound to an explicit Arrow record decoder.
/// Opening performs no network requests; schema inference/decoding may inspect the
/// owned page bytes. Cursor acquisition remains with the provider protocol.
pub struct RecordPage<F> {
    binding: FactoryAccessor<PageInput, F>,
}

impl<F> RecordPage<F> {
    pub(crate) fn new(bytes: Bytes, format: F) -> Self {
        Self {
            binding: FactoryAccessor::new(PageInput(bytes), format),
        }
    }
}

#[async_trait]
impl<F> Accessor for RecordPage<F>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        self.binding.capability()
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.binding.open(request).await
    }
}

#[async_trait]
impl<F> ScanPlanner for RecordPage<F>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}

struct PageInput(Bytes);

#[async_trait]
impl Accessor for PageInput {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let metadata = ByteMetadata {
            source: Some(request.object),
            content_length: Some(self.0.len() as u64),
            version: None,
        };
        let bytes = self.0.clone();
        let chunks = stream::iter((!bytes.is_empty()).then_some(Ok(bytes)));
        Ok(Box::new(StreamReader::new(chunks, metadata)))
    }
}
