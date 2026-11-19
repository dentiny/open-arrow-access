use crate::{
    Accessor, ReadOptionsProvider, ReadRequest, RecordCapability, RecordMetadata, RecordReader,
    Result, ScanPlan, ScanPlanner, ScanStatistics, StatisticsProvider, async_trait,
};

/// Expose a fixed record selection through the common [`ReadRequest`] interface.
///
/// Each operation clones the bound provider request and replaces only its
/// [`crate::ReadOptions`] (batch size and projection). The incoming request's object and
/// byte options are ignored by default; selection and other provider settings
/// remain those of the bound request. Use `with_incoming_byte_options` to forward
/// caller-owned storage I/O settings where the provider exposes them.
/// Construction performs no I/O.
///
/// Metadata operations delegate to the inner accessor, preserving specialized
/// inspection paths and proven-empty results. When the inner accessor implements
/// [`ScanPlanner`], native partitions are forwarded without opening readers.
///
/// ```
/// use open_arrow_access_core::{
///     Accessor, ReadOptionsProvider, ReadRequest, RecordReader, SelectionAccessor,
/// };
///
/// fn select<A>(accessor: A, request: A::Request)
///     -> impl Accessor<Request = ReadRequest, Reader = dyn RecordReader>
/// where
///     A: Accessor<Reader = dyn RecordReader>,
///     A::Request: ReadOptionsProvider + Clone + Sync,
/// {
///     SelectionAccessor::new(accessor, request)
/// }
/// ```
pub struct SelectionAccessor<A: Accessor> {
    inner: A,
    request: A::Request,
    incoming_byte_options: bool,
}

impl<A: Accessor> SelectionAccessor<A> {
    pub fn new(inner: A, request: A::Request) -> Self {
        Self {
            inner,
            request,
            incoming_byte_options: false,
        }
    }

    /// Forward incoming storage I/O settings to provider requests that expose them.
    /// By default the bound request retains its byte options. Opt in when a caller
    /// owns a scan-wide I/O policy, such as a conversion service's request size.
    pub fn with_incoming_byte_options(mut self) -> Self {
        self.incoming_byte_options = true;
        self
    }
}

impl<A: Accessor> SelectionAccessor<A>
where
    A::Request: ReadOptionsProvider + Clone,
{
    fn selected(&self, incoming: ReadRequest) -> A::Request {
        let mut request = self.request.clone();
        *request.read_options_mut() = incoming.options;
        if self.incoming_byte_options
            && let Some(options) = request.byte_read_options_mut()
        {
            *options = incoming.byte_options;
        }
        request
    }
}

#[async_trait]
impl<A> Accessor for SelectionAccessor<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider + Clone + Sync,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        self.inner.metadata(self.selected(request)).await
    }

    async fn inspect_metadata(&self, request: ReadRequest) -> Result<Option<RecordMetadata>> {
        self.inner.inspect_metadata(self.selected(request)).await
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.inner.open(self.selected(request)).await
    }
}

#[async_trait]
impl<A> ScanPlanner for SelectionAccessor<A>
where
    A: ScanPlanner,
    A::Request: ReadOptionsProvider + Clone + Sync,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        self.inner.plan_scan(self.selected(request)).await
    }
}

#[async_trait]
impl<A> StatisticsProvider for SelectionAccessor<A>
where
    A: StatisticsProvider,
    A::Request: ReadOptionsProvider + Clone + Sync,
{
    async fn statistics(&self, request: ReadRequest) -> Result<ScanStatistics> {
        self.inner.statistics(self.selected(request)).await
    }
}
