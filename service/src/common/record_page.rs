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
    /// Expose the native format accessor for format-specific scan planning.
    /// When the chosen format implements ScanPlanner, callers can install the
    /// parallel layer here to use its file-internal framing. This does not make
    /// the provider's continuation/cursor requests independently fetchable.
    pub fn into_file_accessor(self) -> FileAccessor<RecordPageInput, F> {
        self.binding
    }

    pub(crate) fn new(bytes: Bytes, format: F) -> Self {
        Self {
            binding: FileAccessor::from_format(RecordPageInput(bytes), format),
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
