//! Fixed page output schema and assignment of caller-supplied source values.
mod schema;
use arrow_array::{
    LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, StructArray, UInt64Array,
};
use open_arrow_access_core::{Result, SchemaRef, record_batch_with_rows};
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
        self.into_projected_batch(page_schema())
    }

    /// Internal schema comes from a projection resolved against `page_schema`.
    /// Build only selected columns, retaining a row even for an empty projection.
    pub(in crate::bhl) fn into_projected_batch(self, schema: SchemaRef) -> Result<RecordBatch> {
        let mut metadata = Some(self.metadata);
        let columns = schema
            .fields()
            .iter()
            .map(|field| -> arrow_array::ArrayRef {
                match field.name().as_str() {
                    "entity_type" => Arc::new(StringArray::from(vec![self.entity_type])),
                    "entity_id" => Arc::new(UInt64Array::from(vec![self.entity_id])),
                    "page_id" => Arc::new(UInt64Array::from(vec![self.page_id])),
                    "sequence" => Arc::new(UInt64Array::from(vec![self.sequence])),
                    "data" => Arc::new(LargeBinaryArray::from(vec![self.data])),
                    "source_path" => Arc::new(LargeStringArray::from(vec![self.source_path])),
                    "snapshot_id" => Arc::new(StringArray::from(vec![self.snapshot_id])),
                    "source_entry_index" => {
                        Arc::new(UInt64Array::from(vec![self.source_entry_index]))
                    }
                    "metadata" => {
                        Arc::new(metadata.take().expect("unique projected metadata field"))
                    }
                    _ => unreachable!("resolved BHL page schema"),
                }
            })
            .collect();
        record_batch_with_rows(schema, columns, 1).map_err(super::error::page_record)
    }
}

#[cfg(test)]
pub(super) mod tests;
