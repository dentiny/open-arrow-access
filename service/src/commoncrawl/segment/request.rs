use super::super::{CommonCrawl, url::parse_url};
use super::{error::invalid_segment, validation::validate_segment};
use crate::warc::CommonCrawlWarcRequest;
use open_arrow_access_core::{
    ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider, Result,
};

/// Select one exact segment from a crawl-wide WARC inventory.
#[derive(Clone, Debug)]
pub struct CommonCrawlSegmentRequest {
    pub(super) dataset: CommonCrawlWarcRequest,
    segment: String,
}

impl CommonCrawlSegmentRequest {
    pub fn new(crawl: impl Into<String>, segment: impl Into<String>) -> Result<Self> {
        Self::from_source(CommonCrawl::new(crawl)?, segment)
    }
    pub fn from_source(source: CommonCrawl, segment: impl Into<String>) -> Result<Self> {
        let segment = segment.into();
        validate_segment(&segment)?;
        Ok(Self {
            dataset: CommonCrawlWarcRequest::from_source(source),
            segment,
        })
    }
    pub fn from_url(value: &str) -> Result<Self> {
        let url = parse_url(value)?;
        let (crawl, segment) = url
            .path()
            .trim_end_matches('/')
            .strip_prefix("/crawl-data/")
            .and_then(|path| path.split_once("/segments/"))
            .ok_or_else(|| invalid_segment("expected /crawl-data/<crawl>/segments/<segment>/"))?;
        Self::from_source(
            CommonCrawl::new(crawl)?.with_origin(&url.origin().ascii_serialization())?,
            segment,
        )
    }
    pub fn crawl(&self) -> &str {
        self.dataset.crawl()
    }
    pub fn origin(&self) -> &str {
        self.dataset.origin()
    }
    pub fn segment(&self) -> &str {
        &self.segment
    }
    pub fn manifest(&self) -> ObjectRef {
        self.dataset.manifest()
    }
    pub fn with_options(mut self, options: ReadOptions) -> Self {
        self.dataset.options = options;
        self
    }
    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.dataset.byte_options = options;
        self
    }
    pub(super) fn prefix(&self) -> String {
        format!(
            "crawl-data/{}/segments/{}/warc/",
            self.crawl(),
            self.segment
        )
    }
}

impl ReadOptionsProvider for CommonCrawlSegmentRequest {
    fn clone_with_read_options(&self, options: ReadOptions) -> Option<Self> {
        let mut request = self.clone();
        *request.read_options_mut() = options;
        Some(request)
    }

    fn read_options(&self) -> &ReadOptions {
        self.dataset.read_options()
    }
    fn read_options_mut(&mut self) -> &mut ReadOptions {
        self.dataset.read_options_mut()
    }
}
