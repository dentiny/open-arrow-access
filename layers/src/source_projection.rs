use std::sync::Arc;

use arrow_schema::{ArrowError, SchemaRef};
use indexmap::IndexSet;

/// Bind source slots to reader positions in first-use order. Computed slots
/// retain their original indices, beyond the complete source schema's fields.
pub(crate) struct SourceProjection {
    source: SchemaRef,
    columns: Option<IndexSet<usize>>,
}

impl SourceProjection {
    pub(crate) fn new(source: SchemaRef, project: bool) -> Self {
        Self {
            source,
            columns: project.then(IndexSet::new),
        }
    }

    pub(crate) fn bind(&mut self, slot: usize) -> usize {
        match &mut self.columns {
            Some(columns) if slot < self.source.fields().len() => columns.insert_full(slot).0,
            _ => slot,
        }
    }

    pub(crate) fn bind_all(&mut self, slots: &mut [usize]) {
        for slot in slots {
            *slot = self.bind(*slot);
        }
    }

    /// An unbound source slot is absent from the projected reader.
    pub(crate) fn lookup(&self, slot: usize) -> Option<usize> {
        match &self.columns {
            Some(columns) if slot < self.source.fields().len() => columns.get_index_of(&slot),
            _ => Some(slot),
        }
    }

    pub(crate) fn schema(&self) -> Result<SchemaRef, ArrowError> {
        match &self.columns {
            Some(columns) => self
                .source
                .project(&columns.iter().copied().collect::<Vec<_>>())
                .map(Arc::new),
            None => Ok(self.source.clone()),
        }
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        match &self.columns {
            Some(columns) => columns
                .iter()
                .map(|&index| self.source.field(index).name().clone())
                .collect(),
            None => self
                .source
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
        }
    }
}
