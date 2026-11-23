//! Pure discovery of a published record's canonical attachment namespace.
use std::num::NonZeroU64;

use open_arrow_access_core::Result;

use super::{Record, ZenodoMetadata, validation::validate_record};
use crate::{discovery::file_manifest::FileSpec, http_client::files::ResolvedFilesPlan};

pub(super) fn resolve_record(
    record: Record,
    id: NonZeroU64,
) -> Result<ResolvedFilesPlan<ZenodoMetadata>> {
    validate_record(&record, id)?;
    // Use this version's canonical content route, never a latest
    // version link or an arbitrary host supplied in metadata.
    Ok(ResolvedFilesPlan::new(
        format!("api/records/{id}/files/"),
        record
            .files
            .iter()
            .map(|file| FileSpec {
                name: file.key.clone(),
                object: format!("{}/content", file.key).into(),
                content_length: None,
            })
            .collect(),
        ZenodoMetadata {
            id,
            files: record.files,
        },
    ))
}
