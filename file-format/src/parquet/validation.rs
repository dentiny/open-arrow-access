use open_arrow_access_core::{ByteReader, Error, Result};

use super::error::{
    parquet_partition_changed, parquet_partition_input_error, parquet_statistics_row_count_error,
};

pub(super) fn validate_statistics_row_count(rows: i64) -> Result<u64> {
    u64::try_from(rows).map_err(|_| parquet_statistics_row_count_error())
}

pub(super) fn validate_input(
    input: &dyn ByteReader,
    invalid: impl FnOnce() -> Error,
) -> Result<()> {
    if !input.capability().read_with_range || input.metadata().content_length.is_none() {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn validate_partition_input(input: &dyn ByteReader, length: u64) -> Result<()> {
    validate_input(input, parquet_partition_input_error)?;
    if input.metadata().content_length != Some(length) {
        return Err(parquet_partition_changed());
    }
    Ok(())
}
