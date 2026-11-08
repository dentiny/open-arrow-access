use open_arrow_access_core::Result;
use serde::{Deserialize, Deserializer, de::Error as _};

use super::{
    InternetArchiveFile, ItemResponse,
    error::{decode, invalid, not_found, unavailable},
};

pub(super) fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.len() > 100
        || !identifier
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        || !identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(invalid(
            "expected an item identifier using ASCII letters, digits, dots, underscores or hyphens",
        ));
    }
    Ok(())
}

pub(super) fn validate_item(
    response: ItemResponse,
    identifier: &str,
) -> Result<Vec<InternetArchiveFile>> {
    let item = match response {
        ItemResponse::Missing([]) => return Err(not_found()),
        ItemResponse::Item(item) => item,
    };
    if item.is_unavailable {
        return Err(unavailable());
    }
    if item.error.is_some() {
        return Err(decode("Internet Archive returned a metadata error"));
    }
    if item
        .metadata
        .as_ref()
        .map(|metadata| metadata.identifier.as_str())
        != Some(identifier)
    {
        return Err(decode(
            "item identifier differs from the request or is missing",
        ));
    }
    let files = item
        .files
        .ok_or_else(|| decode("item file membership is missing"))?;
    if files.iter().any(|file| !valid_path(&file.name)) {
        return Err(decode("invalid item filename"));
    }
    Ok(files)
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.trim() == path
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
}

pub(super) fn deserialize_size<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Size {
        Number(u64),
        Text(String),
    }
    Option::<Size>::deserialize(deserializer)?
        .map(|size| match size {
            Size::Number(value) => Ok(value),
            Size::Text(value) => value.parse().map_err(D::Error::custom),
        })
        .transpose()
}
