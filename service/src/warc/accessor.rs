use super::{CommonCrawlWarcRequest, manifest::read_manifest};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, ReadRequest, RecordReader, Result,
    async_trait,
};
use open_arrow_access_dataset::{FileSetAccessor, FileSetRequest};

/// Read a crawl's WARC files in manifest order.
/// `manifests` supplies decoded paths; `format` supplies Arrow batches.
/// Both use `crawl-data/...` locators with caller-configured decompression.
#[derive(Debug)]
pub struct CommonCrawlWarcAccessor<M, F> {
    manifests: M,
    files: FileSetAccessor<F>,
}

impl<M, F> CommonCrawlWarcAccessor<M, F> {
    pub fn new(manifests: M, format: F) -> Self {
        Self {
            manifests,
            files: FileSetAccessor::new(format),
        }
    }
}

impl<M: ByteAccessor, F> CommonCrawlWarcAccessor<M, F> {
    /// Collect selected paths without opening record files.
    pub async fn resolve(&self, request: CommonCrawlWarcRequest) -> Result<FileSetRequest> {
        let input = self
            .manifests
            .open(ByteReadRequest::new(request.manifest()).with_options(request.byte_options))
            .await?;
        let objects = read_manifest(input, &request.prefix()).await?;
        Ok(FileSetRequest::new(objects)
            .with_options(request.options)
            .with_byte_options(request.byte_options))
    }
}

#[async_trait]
impl<M, F> Accessor for CommonCrawlWarcAccessor<M, F>
where
    M: ByteAccessor,
    F: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Request = CommonCrawlWarcRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> Capability {
        Capability {
            read: self.manifests.capability().read && self.files.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    async fn open(&self, request: CommonCrawlWarcRequest) -> Result<Box<dyn RecordReader>> {
        self.files.open(self.resolve(request).await?).await
    }
}
