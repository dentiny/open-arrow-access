use std::collections::HashMap;

use arrow_schema::{ArrowError, Fields};

/// An owned, reusable name-to-position index for one immutable field list.
/// Construction takes O(n) time and space; lookups take expected O(1) time.
/// Duplicate names resolve to the first position, matching Schema::index_of.
#[derive(Debug)]
pub struct FieldNameIndex {
    indices: HashMap<String, usize>,
}

impl FieldNameIndex {
    pub fn new(fields: &Fields) -> Self {
        let mut indices = HashMap::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            indices.entry(field.name().clone()).or_insert(index);
        }
        Self { indices }
    }

    /// Look up a borrowed name without allocating or scanning the fields.
    pub fn get(&self, name: &str) -> Option<usize> {
        self.indices.get(name).copied()
    }

    pub fn index_of(&self, name: &str) -> Result<usize, ArrowError> {
        self.get(name)
            .ok_or_else(|| ArrowError::SchemaError(format!("unknown field {name:?}")))
    }
}
