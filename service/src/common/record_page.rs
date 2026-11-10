use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, Capability, ReadRequest, ReaderFactory,
    RecordReader, Result, ScanPlan, ScanPlanner, async_trait, bytes_reader,
};
use open_arrow_access_file_format::FileAccessor;

/// An already resolved API page bound to an explicit Arrow record decoder.
/// Opening performs no network requests; schema inference/decoding may inspect the
/// owned page bytes. Cursor acquisition remains with the provider protocol.
pub struct RecordPage<F> {
    binding: FileAccessor<RecordPageInput, F>,
}

impl<F> RecordPage<F> {
    /// Access the native format configuration. RecordPage delegates scan planning
    /// to this accessor; API continuation requests remain provider-controlled.
    pub fn into_file_accessor(self) -> FileAccessor<RecordPageInput, F> {
        self.binding
    }

    pub(crate) fn new(bytes: Bytes, format: F) -> Self {
        Self {
            binding: FileAccessor::from_format(RecordPageInput(bytes), format),
        }
    }

    /// Plan an application-defined format as one partition without native framing.
    pub async fn plan_whole_page(&self, request: ReadRequest) -> Result<ScanPlan>
    where
        F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
    {
        Ok(ScanPlan::whole_input(self.open(request).await?))
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
    FileAccessor<RecordPageInput, F>: ScanPlanner<Request = ReadRequest>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        self.binding.plan_scan(request).await
    }
}

/// Immutable, already fetched page bytes used by a native format accessor.
/// Instances are created by the provider page adapters. Each open owns an
/// independent sequential cursor and never performs network I/O.
#[derive(Clone)]
pub struct RecordPageInput(Bytes);

#[async_trait]
impl Accessor for RecordPageInput {
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
        Ok(Box::new(bytes_reader(self.0.clone(), Some(request.object))))
    }
}
