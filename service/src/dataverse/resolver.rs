//! Pure discovery of one explicitly selected published dataset version.
use open_arrow_access_core::Result;

use super::validation::{DatasetVersion as RequestedVersion, validate_dataset};
use super::{DataverseFile, DataverseMetadata, Response};
use crate::{discovery::file_manifest::FileSpec, http_client::files::ResolvedFilesPlan};

pub(super) fn resolve_dataset(
    response: Response,
    persistent_id: &str,
    version: &str,
    expected: RequestedVersion,
    has_credentials: bool,
) -> Result<ResolvedFilesPlan<DataverseMetadata>> {
    validate_dataset(&response, persistent_id, expected, has_credentials)?;
    let files: Vec<_> = response
        .data
        .files
        .into_iter()
        .map(|file| DataverseFile {
            name: if file.directory_label.is_empty() {
                file.label
            } else {
                format!(
                    "{}/{}",
                    file.directory_label.trim_end_matches('/'),
                    file.label
                )
            },
            id: file.data_file.id,
            size: file
                .data_file
                .original_file_size
                .unwrap_or(file.data_file.filesize),
        })
        .collect();
    let mappings = files
        .iter()
        .map(|file| FileSpec {
            name: file.name.clone(),
            object: file.id.to_string().into(),
            content_length: Some(file.size),
        })
        .collect();
    Ok(ResolvedFilesPlan::new(
        "api/access/datafile/",
        mappings,
        DataverseMetadata {
            persistent_id: persistent_id.to_owned(),
            version: version.to_owned(),
            files,
        },
    )
    .with_query(&[("format", "original")]))
}
