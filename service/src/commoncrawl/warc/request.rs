use super::super::CommonCrawl;
use open_arrow_access_core::{
    ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider, Result,
};

/// Select all WARC files in one Common Crawl release, in inventory order.
#[derive(Clone, Debug)]
pub struct CommonCrawlWarcRequest {
    source: CommonCrawl,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

impl CommonCrawlWarcRequest {
    pub fn new(crawl: impl Into<String>) -> Result<Self> {
        Ok(Self::from_source(CommonCrawl::new(crawl)?))
    }
    pub fn from_source(source: CommonCrawl) -> Self {
        Self {
            source,
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        }
    }
    pub fn from_url(value: &str) -> Result<Self> {
        Ok(Self::from_source(CommonCrawl::from_url(value)?))
    }
    pub fn crawl(&self) -> &str {
        self.source.crawl()
    }
    pub fn origin(&self) -> &str {
        self.source.origin()
    }
    pub fn manifest(&self) -> ObjectRef {
        ObjectRef::new(format!("crawl-data/{}/warc.paths.gz", self.crawl()))
    }
    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }
    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }
    pub(super) fn prefix(&self) -> String {
        format!("crawl-data/{}/segments/", self.crawl())
    }
}

impl ReadOptionsProvider for CommonCrawlWarcRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}
