use std::num::NonZeroU64;

use open_arrow_access_core::Result;

use super::{Record, error::decode};
use crate::filename::valid_filename_component;

pub(super) fn validate_record(record: &Record, id: NonZeroU64) -> Result<()> {
    if record.id != id.get() {
        return Err(decode("record ID differs from the requested version"));
    }
    if record
        .files
        .iter()
        .any(|file| !valid_filename_component(&file.key))
    {
        return Err(decode("invalid attachment key"));
    }
    Ok(())
}
