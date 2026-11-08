use open_arrow_access_core::{Error, ErrorKind, Result};

use super::{url::parse_url, validation::validate_crawl};

/// One Common Crawl release and the origin hosting its objects.
#[derive(Clone, Debug)]
pub struct CommonCrawl {
    /// Release ID in `CC-MAIN-YYYY-NN` form.
    crawl: String,
    /// HTTP(S) scheme and host, with an optional port and no path.
    origin: String,
}

impl CommonCrawl {
    pub fn new(crawl: impl Into<String>) -> Result<Self> {
        let crawl = crawl.into();
        validate_crawl(&crawl)?;
        Ok(Self {
            crawl,
            origin: "https://data.commoncrawl.org".into(),
        })
    }

    /// Select a release URL such as `https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/`.
    pub fn from_url(value: &str) -> Result<Self> {
        let url = parse_url(value)?;
        let path = url.path().trim_end_matches('/');
        let crawl = path.strip_prefix("/crawl-data/").ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "configure_commoncrawl",
                "expected /crawl-data/<crawl>/",
            )
        })?;
        Self::new(crawl)?.with_origin(&url.origin().ascii_serialization())
    }

    pub fn with_origin(mut self, origin: &str) -> Result<Self> {
        let url = parse_url(origin)?;
        if url.path() != "/" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "configure_commoncrawl",
                "expected an HTTP(S) origin without a path",
            ));
        }
        self.origin = url.origin().ascii_serialization();
        Ok(self)
    }

    pub fn crawl(&self) -> &str {
        &self.crawl
    }
    pub fn origin(&self) -> &str {
        &self.origin
    }
}
