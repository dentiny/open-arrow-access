use crate::commoncrawl::CommonCrawl;
use open_arrow_access_core::{
    ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider, Result,
};

/// Select all WARC files in one Common Crawl release, in inventory order.
#[derive(Clone, Debug)]
pub struct CommonCrawlWarcRequest {
    source: Source,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

#[derive(Clone, Debug)]
enum Source {
    Crawl(CommonCrawl),
    Manifest {
        crawl: String,
        origin: String,
        object: ObjectRef,
    },
}

impl CommonCrawlWarcRequest {
    pub fn new(crawl: impl Into<String>) -> Result<Self> {
        Ok(Self::from_source(CommonCrawl::new(crawl)?))
    }
    pub fn from_source(source: CommonCrawl) -> Self {
        Self {
            source: Source::Crawl(source),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        }
    }
    pub fn from_url(value: &str) -> Result<Self> {
        Ok(Self::from_source(CommonCrawl::from_url(value)?))
    }
    /// Select a crawl's WARC files from a custom HTTP(S) inventory.
    /// Configure origin-rooted accessors with `origin()`. Entries remain WARC keys
    /// rooted at that origin, not paths relative to the manifest's directory.
    /// URLs follow the same canonical rules as `from_url`; directory URLs are rejected.
    pub fn from_manifest_url(crawl: impl Into<String>, value: &str) -> Result<Self> {
        let crawl = CommonCrawl::new(crawl)?;
        let url = crate::commoncrawl::url::parse_url(value)?;
        if url.path().ends_with('/') {
            return Err(open_arrow_access_core::Error::new(
                open_arrow_access_core::ErrorKind::InvalidInput,
                "configure_commoncrawl_manifest",
                "expected an HTTP(S) manifest file URL",
            ));
        }
        Ok(Self {
            source: Source::Manifest {
                crawl: crawl.crawl().to_owned(),
                origin: url.origin().ascii_serialization(),
                object: ObjectRef::new(url.path().trim_start_matches('/')),
            },
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        })
    }
    pub fn crawl(&self) -> &str {
        match &self.source {
            Source::Crawl(source) => source.crawl(),
            Source::Manifest { crawl, .. } => crawl,
        }
    }
    pub fn origin(&self) -> &str {
        match &self.source {
            Source::Crawl(source) => source.origin(),
            Source::Manifest { origin, .. } => origin,
        }
    }
    pub fn manifest(&self) -> ObjectRef {
        match &self.source {
            Source::Crawl(source) => {
                ObjectRef::new(format!("crawl-data/{}/warc.paths.gz", source.crawl()))
            }
            Source::Manifest { object, .. } => object.clone(),
        }
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
