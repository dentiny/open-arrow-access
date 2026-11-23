//! Pure discovery of an item's canonical download namespace.
use open_arrow_access_core::Result;

use super::{InternetArchiveMetadata, ItemResponse, validation::extract_items_and_validate};
use crate::{discovery::file_manifest::FileSpec, http_client::files::ResolvedFilesPlan};

pub(super) fn resolve_item(
    response: ItemResponse,
    identifier: &str,
    has_credentials: bool,
) -> Result<ResolvedFilesPlan<InternetArchiveMetadata>> {
    let files = extract_items_and_validate(response, identifier, has_credentials)?;
    Ok(ResolvedFilesPlan::new(
        format!("download/{identifier}/"),
        files
            .iter()
            .map(|file| FileSpec {
                name: file.name.clone(),
                object: file.name.clone().into(),
                content_length: None,
            })
            .collect(),
        InternetArchiveMetadata {
            identifier: identifier.into(),
            files,
        },
    ))
}
