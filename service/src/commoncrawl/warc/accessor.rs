use super::{CommonCrawlWarcRequest, manifest::read_manifest};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, ReadRequest, RecordReader, Result,
    async_trait,
};
use open_arrow_access_dataset::{FileSetAccessor, FileSetRequest};

/// Resolve a crawl-wide WARC inventory and read files sequentially.
///
/// `manifests` must expose decoded `warc.paths.gz` bytes. Supply gzip separately
/// for the official inventory. `format` handles individual WARC files, including
/// any decompression. Both accessors use origin-rooted `crawl-data/...` locators.
/// `format` emits Arrow batches; native `WarcAccessor` records need a caller-provided
/// Arrow conversion adapter before this boundary.
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
    /// Resolve selected membership before opening any record file.
    /// A bounded line buffer streams the inventory; selected locators are retained.
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
