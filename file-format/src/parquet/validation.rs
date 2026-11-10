use open_arrow_access_core::{ByteReader, Result};

use super::error::parquet_partition_input_error;

pub(super) fn validate_input(input: &dyn ByteReader) -> Result<()> {
    if !input.capability().read_with_range || input.metadata().content_length.is_none() {
        return Err(parquet_partition_input_error());
    }
    Ok(())
}
