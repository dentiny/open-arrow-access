//! Fixed page output schema and assignment of caller-supplied source values.
mod schema;
use arrow_array::{
    LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use open_arrow_access_core::Result;
pub use schema::page_schema;
use std::sync::Arc;

/// Values for one output page. Metadata must be supplied as one populated row
/// matching the fixed schema. Missing metadata is an error, never a null default.
/// This type does not fetch or join source metadata.
pub struct BhlPageRecord<'a> {
    pub entity_type: &'a str,
    pub entity_id: u64,
    pub page_id: u64,
    pub sequence: u64,
    pub data: &'a [u8],
    pub source_path: &'a str,
    pub snapshot_id: &'a str,
    pub source_entry_index: Option<u64>,
    pub metadata: StructArray,
}

impl BhlPageRecord<'_> {
    /// Assign every source value, including the supplied metadata array, to a row.
    /// Arrow validates all field types, row lengths and required metadata validity.
    pub fn into_batch(self) -> Result<RecordBatch> {
        RecordBatch::try_new(
            page_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.entity_type])),
                Arc::new(UInt64Array::from(vec![self.entity_id])),
                Arc::new(UInt64Array::from(vec![self.page_id])),
                Arc::new(UInt64Array::from(vec![self.sequence])),
                Arc::new(LargeBinaryArray::from(vec![self.data])),
                Arc::new(LargeStringArray::from(vec![self.source_path])),
                Arc::new(StringArray::from(vec![self.snapshot_id])),
                Arc::new(UInt64Array::from(vec![self.source_entry_index])),
                Arc::new(self.metadata),
            ],
        )
        .map_err(super::error::page_record)
    }
}

#[cfg(test)]
pub(super) mod tests;
