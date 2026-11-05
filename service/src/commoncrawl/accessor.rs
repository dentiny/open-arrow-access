use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, ReadRequest, RecordReader, Result,
    async_trait,
};
use open_arrow_access_dataset::{FileSetAccessor, FileSetRequest};

use super::{CommonCrawlSegmentRequest, manifest::read_manifest};

/// Resolve a Common Crawl segment and read its files in manifest order.
///
/// `manifests` must expose decoded manifest bytes. For official `warc.paths.gz`,
/// supply a gzip layer over an origin-rooted byte accessor. `format` handles
/// file decoding separately, normally gzip followed by a WARC accessor. Both
/// accessors interpret canonical `crawl-data/...` locators from the origin root.
#[derive(Clone, Debug)]
pub struct CommonCrawlSegmentAccessor<M, F> {
    manifests: M,
    files: FileSetAccessor<F>,
}

impl<M, F> CommonCrawlSegmentAccessor<M, F> {
    pub fn new(manifests: M, format: F) -> Self {
        Self {
            manifests,
            files: FileSetAccessor::new(format),
        }
    }
}

impl<M: ByteAccessor, F> CommonCrawlSegmentAccessor<M, F> {
    /// Resolve the complete selected membership before opening any record file.
    ///
    /// The manifest is read incrementally with a bounded line buffer; selected
    /// locators are retained for the file-set reader. Their original order and
    /// spelling are preserved. Invalid entries or duplicate selected paths fail.
    pub async fn resolve(&self, request: CommonCrawlSegmentRequest) -> Result<FileSetRequest> {
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
impl<M, F> Accessor for CommonCrawlSegmentAccessor<M, F>
where
    M: ByteAccessor,
    F: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Request = CommonCrawlSegmentRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.manifests.capability().read && self.files.capability().read,
            ..Capability::default()
        }
    }

    async fn open(&self, request: CommonCrawlSegmentRequest) -> Result<Box<dyn RecordReader>> {
        self.files.open(self.resolve(request).await?).await
    }
}
