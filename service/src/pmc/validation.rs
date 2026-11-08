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
