use open_arrow_access_core::{ByteMetadata, ByteReader, Error, Result};

use super::error::{parquet_partition_changed, parquet_partition_input_error};

pub(super) fn validate_input(
    input: &dyn ByteReader,
    invalid: impl FnOnce() -> Error,
) -> Result<()> {
    if !input.capability().read_with_range || input.metadata().content_length.is_none() {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn validate_partition_input(
    input: &dyn ByteReader,
    identity: &ByteMetadata,
) -> Result<()> {
    validate_input(input, parquet_partition_input_error)?;
    if input.metadata().content_length != identity.content_length
        || input.metadata().version != identity.version
    {
        return Err(parquet_partition_changed());
    }
    Ok(())
}
