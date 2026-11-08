use open_arrow_access_core::{Error, ErrorKind, Result};

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

#[cfg(test)]
#[path = "../../tests/unit/commoncrawl_validation.rs"]
mod tests;
