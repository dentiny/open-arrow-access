use open_arrow_access_core::Result;
use url::Url;

use super::error::invalid_url;
use crate::http_client::validation::is_http_endpoint;

pub(super) fn validate_crawl(crawl: &str) -> Result<()> {
    let parts = crawl
        .strip_prefix("CC-MAIN-")
        .and_then(|value| value.split_once('-'));
    if !matches!(parts, Some((year, run)) if is_digits(year, 4) && is_digits(run, 2)) {
        return Err(super::error::invalid_crawl());
    }
    Ok(())
}

fn is_digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit())
}

pub(super) fn validate_origin_path(url: &Url, operation: &'static str) -> Result<()> {
    if url.path() != "/" {
        return Err(super::error::invalid_origin_path(operation));
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

pub(super) fn crawl_from_url(url: &Url) -> Result<&str> {
    let path = url.path().trim_end_matches('/');
    let crawl = path
        .strip_prefix("/crawl-data/")
        .ok_or_else(super::error::invalid_crawl_path)?;

    Ok(crawl)
}
