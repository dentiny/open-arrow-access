use std::num::NonZeroU64;

use open_arrow_access_core::Result;
use url::Url;

use super::{
    Record,
    error::{decode, invalid},
};

pub(super) fn validate_endpoint(endpoint: &str) -> Result<Url> {
    let endpoint = Url::parse(endpoint).map_err(|_| invalid("invalid endpoint"))?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.path() != "/"
    {
        return Err(invalid("expected an HTTP(S) origin without credentials"));
    }
    Ok(endpoint)
}

pub(super) fn validate_record(record: &Record, id: NonZeroU64) -> Result<()> {
    if record.id != id.get() {
        return Err(decode("record ID differs from the requested version"));
    }
    if record.files.iter().any(|file| !valid_key(&file.key)) {
        return Err(decode("invalid attachment key"));
    }
    Ok(())
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key != "."
        && key != ".."
        && !key.contains(['/', '\\'])
        && !key.chars().any(char::is_control)
        && key.trim() == key
}
