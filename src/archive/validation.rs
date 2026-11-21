use crate::{ReadOptions, Result, SchemaRef};

use super::{archive_schema, error};

pub(super) fn projected_schema(options: &ReadOptions) -> Result<SchemaRef> {
    Ok(options.resolve_projection(&archive_schema())?.schema)
}

pub(super) fn validate_chunk(chunk: &[u8]) -> Result<()> {
    if chunk.is_empty() {
        return Err(error::empty_chunk());
    }
    Ok(())
}
