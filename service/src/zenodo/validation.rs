use std::num::NonZeroU64;

use open_arrow_access_core::Result;

use super::{Record, error::decode};

pub(super) fn validate_record(record: &Record, id: NonZeroU64) -> Result<()> {
    if record.id != id.get() {
        return Err(decode("record ID differs from the requested version"));
    }
    Ok(())
}
