use super::CommonCrawlSegmentRequest;
use crate::warc::CommonCrawlWarcAccessor;
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, ReadRequest, RecordReader, Result, ScanPlan, ScanPlanner,
    async_trait,
};
use open_arrow_access_dataset::file_set::FileSetRequest;

/// Select one segment and read its WARC files in manifest order.
#[derive(Debug)]
pub struct CommonCrawlSegmentAccessor<M, F> {
    warc: CommonCrawlWarcAccessor<M, F>,
}

impl<M, F> CommonCrawlSegmentAccessor<M, F> {
    pub fn new(manifests: M, format: F) -> Self {
        Self {
            warc: CommonCrawlWarcAccessor::new(manifests, format),
        }
    }
}

impl<M: ByteAccessor, F> CommonCrawlSegmentAccessor<M, F> {
    pub async fn resolve(&self, request: CommonCrawlSegmentRequest) -> Result<FileSetRequest> {
        let prefix = request.prefix();
        self.warc.resolve_prefix(request.dataset, &prefix).await
    }
}

#[async_trait]
impl<M, F> Accessor for CommonCrawlSegmentAccessor<M, F>
where
    M: ByteAccessor,
    F: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Request = CommonCrawlSegmentRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: self.warc.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: CommonCrawlSegmentRequest) -> Result<Box<dyn RecordReader>> {
        self.warc.files.open(self.resolve(request).await?).await
    }
}

#[async_trait]
impl<M, F> ScanPlanner for CommonCrawlSegmentAccessor<M, F>
where
    M: ByteAccessor,
    F: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    async fn plan_scan(&self, request: CommonCrawlSegmentRequest) -> Result<ScanPlan> {
        self.warc
            .files
            .plan_scan(self.resolve(request).await?)
            .await
    }
}

impl<M, B>
    CommonCrawlSegmentAccessor<M, open_arrow_access_file_format::warc::WarcArrowAccessor<B>>
{
    /// Use the native Arrow WARC view. Byte layers must expose decoded WARC bytes;
    /// install gzip decompression explicitly for compressed record files.
    pub fn from_warc_bytes(manifests: M, bytes: B) -> Self {
        Self::new(
            manifests,
            open_arrow_access_file_format::warc::WarcArrowAccessor::new(bytes),
        )
    }
}
