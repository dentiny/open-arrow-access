use arrow_schema::SchemaRef;
use open_arrow_access_core::{Result, SourceColumn};

pub(crate) fn validate_source_object_column(
    input: &SchemaRef,
    columns: &[SourceColumn],
) -> Result<()> {
    if columns.contains(&SourceColumn::Object)
        && input.index_of(SourceColumn::Object.name()).is_ok()
    {
        return Err(crate::error::source_object_collision());
    }
    Ok(())
}
#[cfg(feature = "warc")]
pub(crate) fn validate_source_columns(columns: &[SourceColumn], input: &SchemaRef) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for &column in columns {
        if !seen.insert(column) || input.index_of(column.name()).is_ok() {
            return Err(crate::error::source_column_conflict());
        }
    }
    Ok(())
}
#[cfg(feature = "warc")]
pub(crate) fn validate_positioned_source(
    input: &dyn open_arrow_access_core::ByteReader,
) -> Result<()> {
    if input.position().is_none() {
        return Err(crate::error::source_position_unavailable(
            input.metadata().source.as_ref(),
        ));
    }
    Ok(())
}
#[cfg(feature = "warc")]
pub(crate) fn require_source_position(
    position: Option<open_arrow_access_core::BytePosition>,
) -> Result<open_arrow_access_core::BytePosition> {
    position.ok_or_else(crate::error::source_position_lost)
}

pub(crate) fn require_source_field(
    batch: &open_arrow_access_core::RecordBatch,
    name: &str,
) -> Result<arrow_array::ArrayRef> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(crate::error::missing_source_field)
}
