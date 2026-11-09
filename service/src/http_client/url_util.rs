use super::{error, validation};
use open_arrow_access_core::Result;
use url::Url;

pub(crate) fn resolve_origin(value: &str, operation: &'static str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| error::invalid_origin(operation))?;
    validation::validate_origin(&url, operation)?;
    Ok(url)
}
