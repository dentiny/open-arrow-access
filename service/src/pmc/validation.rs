use super::error::decode;
use open_arrow_access_core::Result;

pub(super) fn validate_pmcid(pmcid: &str, path: &str) -> Result<()> {
    let value = pmcid.trim();
    let digits = if value
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("PMC"))
    {
        &value[3..]
    } else {
        value
    };
    if digits.is_empty()
        || !digits.bytes().all(|b| b.is_ascii_digit())
        || digits.bytes().all(|b| b == b'0')
    {
        return Err(decode(format!("{path}: invalid PMCID")));
    }
    Ok(())
}

pub(super) fn validate_article_version(version: Option<u32>, path: &str) -> Result<()> {
    if version == Some(0) {
        return Err(decode(format!("{path}: article version must be positive")));
    }
    Ok(())
}

pub(super) fn validate_metadata(
    batch: &open_arrow_access_core::RecordBatch,
    path: &str,
) -> Result<()> {
    use arrow_array::{Array, cast::AsArray, types::UInt32Type};

    validate_pmcid(batch["pmcid"].as_string::<i32>().value(0), path)?;
    let version = batch["version"].as_primitive::<UInt32Type>();
    validate_article_version((!version.is_null(0)).then(|| version.value(0)), path)?;
    let media = batch["media_urls"].as_list::<i32>();
    if !media.is_null(0) && media.value(0).null_count() != 0 {
        return Err(decode(format!("{path}: media URLs must be strings")));
    }
    Ok(())
}
