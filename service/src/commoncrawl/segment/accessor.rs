use super::{super::CommonCrawlWarcAccessor, CommonCrawlSegmentRequest};
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, ReadRequest, RecordReader, Result, async_trait,
};
use open_arrow_access_dataset::FileSetRequest;

/// Select a segment while streaming the shared crawl-wide WARC inventory.
/// Only selected locators are retained; reading uses the existing sequential FileSet.
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
