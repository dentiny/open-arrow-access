use open_arrow_access_core::{Error, ErrorKind, Result};
use url::Url;

use super::error::invalid_url;
use crate::http_client::validation::is_http_endpoint;

pub(super) fn validate_crawl(crawl: &str) -> Result<()> {
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
    Ok(())
}

fn is_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit())
}

pub(super) fn validate_origin_path(url: &Url, operation: &'static str) -> Result<()> {
    if url.path() != "/" {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            operation,
            "expected an HTTP(S) origin without a path",
        ));
    }
    Ok(())
}

pub(super) fn validate_url(value: &str, url: &Url) -> Result<()> {
    if !is_http_endpoint(url)
        || value.trim() != value
        || value.chars().any(char::is_control)
        || value.contains(['%', '\\'])
        || value.split('/').any(|part| matches!(part, "." | ".."))
        || url
            .path()
            .strip_prefix('/')
            .unwrap_or(url.path())
            .contains("//")
    {
        return Err(invalid_url());
    }
    Ok(())
}
