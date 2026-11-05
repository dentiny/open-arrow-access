use open_arrow_access_core::{
    ByteReadOptions, Error, ErrorKind, ObjectRef, ReadOptions, ReadOptionsProvider, Result,
};
use url::Url;

/// Select one Common Crawl segment. File paths remain manifest-relative.
#[derive(Clone, Debug)]
pub struct CommonCrawlSegmentRequest {
    crawl: String,
    segment: String,
    origin: String,
    pub options: ReadOptions,
    pub byte_options: ByteReadOptions,
}

impl CommonCrawlSegmentRequest {
    pub fn new(crawl: impl Into<String>, segment: impl Into<String>) -> Result<Self> {
        let crawl = crawl.into();
        let segment = segment.into();
        let crawl_parts = crawl
            .strip_prefix("CC-MAIN-")
            .and_then(|v| v.split_once('-'));
        if !matches!(crawl_parts, Some((year, run)) if digits(year, 4) && digits(run, 2))
            || !matches!(segment.split_once('.'), Some((timestamp, part)) if !timestamp.is_empty() && !part.is_empty() && timestamp.bytes().all(|b| b.is_ascii_digit()) && part.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(invalid_request(
                "expected CC-MAIN-YYYY-NN and a numeric segment identifier",
            ));
        }
        Ok(Self {
            crawl,
            segment,
            origin: "https://data.commoncrawl.org".to_owned(),
            options: ReadOptions::default(),
            byte_options: ByteReadOptions::default(),
        })
    }

    /// Parse a segment URL without normalizing path aliases or URL suffixes.
    pub fn from_url(value: &str) -> Result<Self> {
        let url =
            Url::parse(value).map_err(|e| invalid_request("invalid segment URL").with_source(e))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || value.trim() != value
            || value.chars().any(char::is_control)
            || value.ends_with("//")
            || value.contains(['%', '\\'])
            || value.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(invalid_request(
                "expected an HTTP(S) segment URL without credentials, query, fragment or path aliases",
            ));
        }
        let parts = url
            .path()
            .trim_end_matches('/')
            .split('/')
            .collect::<Vec<_>>();
        if parts.len() != 5
            || !parts[0].is_empty()
            || parts[1] != "crawl-data"
            || parts[3] != "segments"
        {
            return Err(invalid_request(
                "expected /crawl-data/<crawl>/segments/<segment>/",
            ));
        }
        let mut request = Self::new(parts[2], parts[4])?;
        request.origin = url.origin().ascii_serialization();
        Ok(request)
    }

    pub fn crawl(&self) -> &str {
        &self.crawl
    }

    pub fn segment(&self) -> &str {
        &self.segment
    }

    /// Origin used when constructing a matching HTTP byte accessor.
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Official crawl-wide manifest locator, relative to the origin root.
    pub fn manifest(&self) -> ObjectRef {
        ObjectRef::new(format!("crawl-data/{}/warc.paths.gz", self.crawl))
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
        format!("crawl-data/{}/segments/{}/warc/", self.crawl, self.segment)
    }
}

impl ReadOptionsProvider for CommonCrawlSegmentRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}

fn digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_digit())
}

fn invalid_request(message: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "select_commoncrawl_segment",
        message,
    )
}
