use crate::commoncrawl::CommonCrawl;
use open_arrow_access_core::{
    ByteReadOptions, ObjectRef, ReadOptions, ReadOptionsProvider, Result,
};

/// Select all WARC files in one Common Crawl release, in inventory order.
#[derive(Clone, Debug)]
pub struct CommonCrawlWarcRequest {
    source: CommonCrawl,
    manifest_url: Option<url::Url>,
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
            manifest_url: None,
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
    /// Override the inventory location, retaining the selected crawl and its WARC origin.
    /// Configure the manifest byte accessor with `manifest_origin()`. Manifest entries
    /// remain origin-rooted WARC keys, not paths relative to this URL's directory.
    /// The URL must be canonical HTTP(S), without credentials, query or fragment;
    /// percent encoding, traversal, and directory URLs are rejected.
    pub fn with_manifest_url(mut self, value: &str) -> Result<Self> {
        let url = crate::commoncrawl::url::parse_url(value)?;
        if url.path().ends_with('/') {
            return Err(open_arrow_access_core::Error::new(
                open_arrow_access_core::ErrorKind::InvalidInput,
                "configure_commoncrawl_manifest",
                "expected an HTTP(S) manifest file URL",
            ));
        }
        self.manifest_url = Some(url);
        Ok(self)
    }
    /// Origin to configure on the manifest byte accessor; may differ from `origin()`.
    pub fn manifest_origin(&self) -> String {
        self.manifest_url.as_ref().map_or_else(
            || self.origin().to_owned(),
            |url| url.origin().ascii_serialization(),
        )
    }
    pub fn manifest(&self) -> ObjectRef {
        match &self.manifest_url {
            Some(url) => ObjectRef::new(url.path().trim_start_matches('/')),
            None => ObjectRef::new(format!("crawl-data/{}/warc.paths.gz", self.crawl())),
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
