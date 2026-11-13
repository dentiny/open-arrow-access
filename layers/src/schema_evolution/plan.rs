use std::sync::Arc;

use arrow_schema::{FieldRef, SchemaRef};
use datafusion::physical_expr::PhysicalExpr;
use indexmap::IndexSet;
use open_arrow_access_core::Result;

use super::ColumnEvolution;

mod builder;

pub(super) struct SchemaEvolutionPlan {
    pub(super) source_schema: SchemaRef,
    pub(super) target_schema: SchemaRef,
    // Source column indices checked when a batch has a different schema instance.
    pub(super) affected: Vec<usize>,
    // Runtime operations in caller order; renames are resolved during planning.
    pub(super) steps: Vec<EvolutionStep>,
    // Source-column or computed-value slot for each field in target_schema.
    pub(super) output_indices: Vec<usize>,
    pub(super) lifetimes: crate::value_lifetime::ValueLifetimePlan,
}

pub(super) enum EvolutionStep {
    /// Evaluate the compiled expression using `input`'s dependency schema and
    /// array slots, validate against `field`, and store the result at `index`.
    Add {
        index: usize,
        field: FieldRef,
        expression: Arc<dyn PhysicalExpr>,
        input: (SchemaRef, Vec<usize>),
    },
    /// Cast the current array at `index` to `field.data_type()` and cache the result.
    Cast { index: usize, field: FieldRef },
    /// Drop the slot's cached array; the source batch itself remains unchanged.
    Remove(usize),
}

/// Assign compact input indices on first use; computed slots stay unchanged.
struct SourceProjection {
    source_len: usize,
    columns: Option<IndexSet<usize>>,
}

impl SourceProjection {
    fn new(source_len: usize, project_source: bool) -> Self {
        Self {
            source_len,
            columns: project_source.then(IndexSet::new),
        }
    }

    fn into_schema(self, source: &SchemaRef) -> Result<SchemaRef> {
        match self.columns {
            Some(columns) => source
                .project(&columns.into_iter().collect::<Vec<_>>())
                .map(Arc::new)
                .map_err(crate::error::evolution_batch),
            None => Ok(source.clone()),
        }
    }

    fn bind(&mut self, slot: usize) -> usize {
        match &mut self.columns {
            Some(columns) if slot < self.source_len => columns.insert_full(slot).0,
            _ => slot,
        }
    }

    fn lookup(&self, slot: usize) -> Option<usize> {
        match &self.columns {
            Some(columns) if slot < self.source_len => columns.get_index_of(&slot),
            _ => Some(slot),
        }
    }
}

impl SchemaEvolutionPlan {
    pub(super) fn new(
        changes: &[ColumnEvolution],
        source: &SchemaRef,
        options: &open_arrow_access_core::ReadOptions,
        project_source: bool,
    ) -> Result<Self> {
        let mut planner = builder::EvolutionPlanner::new(source, project_source)?;
        for change in changes {
            planner.apply(change)?;
        }
        planner.finish(options)
    }

    pub(super) fn source_columns(&self) -> Vec<String> {
        self.source_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }
}
