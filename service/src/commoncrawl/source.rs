use open_arrow_access_core::{Error, ErrorKind, Result};

use super::url::parse_url;

/// One Common Crawl release and the origin hosting its objects.
#[derive(Clone, Debug)]
pub struct CommonCrawl {
    crawl: String,
    origin: String,
}

impl CommonCrawl {
    pub fn new(crawl: impl Into<String>) -> Result<Self> {
        let crawl = crawl.into();
        let parts = crawl
            .strip_prefix("CC-MAIN-")
            .and_then(|value| value.split_once('-'));
        if !matches!(parts, Some((year, run)) if is_digits(year, 4) && is_digits(run, 2)) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "configure_commoncrawl",
                "expected CC-MAIN-YYYY-NN",
            ));
        }
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

fn is_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit())
}
