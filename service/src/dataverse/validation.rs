use open_arrow_access_core::Result;

use crate::filename::valid_filename_component;

use super::{
    Response,
    error::{decode, invalid, unsupported},
};

pub(super) fn validate_request(persistent_id: &str, version: &str) -> Result<(u64, u64)> {
    let identifier = persistent_id
        .strip_prefix("doi:")
        .or_else(|| persistent_id.strip_prefix("hdl:"));
    if identifier.is_none_or(|id| id.is_empty())
        || persistent_id.trim() != persistent_id
        || persistent_id.chars().any(char::is_control)
    {
        return Err(invalid("expected a DOI or Handle persistent identifier"));
    }
    let (major, minor) = version
        .split_once('.')
        .ok_or_else(|| invalid("expected an explicit major.minor version"))?;
    if major.is_empty()
        || minor.is_empty()
        || !major.bytes().all(|b| b.is_ascii_digit())
        || !minor.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid("expected an explicit major.minor version"));
    }
    let major = major
        .parse::<u64>()
        .map_err(|_| invalid("invalid major version"))?;
    let minor = minor
        .parse::<u64>()
        .map_err(|_| invalid("invalid minor version"))?;
    if major == 0 {
        return Err(invalid("published major version must be positive"));
    }
    Ok((major, minor))
}

pub(super) fn validate_dataset(
    response: &Response,
    persistent_id: &str,
    expected: (u64, u64),
) -> Result<()> {
    let data = &response.data;
    let same_id = if persistent_id.starts_with("doi:") {
        data.dataset_persistent_id
            .eq_ignore_ascii_case(persistent_id)
    } else {
        data.dataset_persistent_id == persistent_id
    };
    if response.status != "OK" {
        return Err(decode("Dataverse response status is not OK"));
    }
    if !same_id {
        return Err(decode("dataset ID differs from the requested identifier"));
    }
    if (data.version_number, data.version_minor_number) != expected {
        return Err(decode("dataset version differs from the requested version"));
    }
    if data.version_state != "RELEASED" {
        return Err(decode("dataset version is not published"));
    }
    for file in &data.files {
        if file.restricted {
            return Err(unsupported("restricted Dataverse files are not supported"));
        }
        if file.data_file.tabular_data && file.data_file.original_file_size.is_none() {
            return Err(unsupported(
                "original byte length is absent for an ingested tabular file",
            ));
        }
        if file.data_file.id == 0 {
            return Err(decode("file ID must be nonzero"));
        }
        if !valid_filename_component(&file.label) {
            return Err(decode("invalid logical filename"));
        }
        if !file.directory_label.is_empty()
            && !file
                .directory_label
                .trim_end_matches('/')
                .split('/')
                .all(valid_filename_component)
        {
            return Err(decode("invalid directory label"));
        }
    }
    Ok(())
}
