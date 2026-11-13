use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_schema::{Field, FieldRef, Schema, SchemaRef};
use datafusion::{common::DFSchema, physical_expr::PhysicalExpr};
use indexmap::IndexSet;
use open_arrow_access_core::Result;

use super::ColumnEvolution;
use crate::{expression::ExpressionPlanner, validation};

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
    Add {
        index: usize,
        field: FieldRef,
        expression: Arc<dyn PhysicalExpr>,
        input: (SchemaRef, Vec<usize>),
    },
    Cast {
        index: usize,
        field: FieldRef,
    },
    Remove(usize),
}

/// Assign compact input indices on first use; computed slots stay unchanged.
struct SourceProjection {
    source_len: usize,
    columns: Option<IndexSet<usize>>,
}

impl SourceProjection {
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
        let mut names: HashMap<_, _> = source
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name().as_str(), index))
            .collect();
        validation::validate_evolution_source(names.len(), source.fields().len())?;
        let mut fields = source.fields().to_vec();
        let mut inputs = SourceProjection {
            source_len: fields.len(),
            columns: project_source.then(IndexSet::new),
        };
        let mut affected = HashSet::new();
        let mut steps = Vec::new();
        let mut planner = None;
        for change in changes {
            let index = names.get(change.name()).copied();
            if let Some(index) = index
                && index < source.fields().len()
            {
                affected.insert(index);
            }
            let resolve = || index.ok_or_else(|| crate::error::evolution_source(change.name()));
            match change {
                ColumnEvolution::AddColumn {
                    name,
                    data_type,
                    expression,
                } => {
                    validation::validate_evolution_target(None, index)?;
                    let mut indices = expression
                        .column_refs()
                        .into_iter()
                        .map(|column| {
                            names
                                .get(column.name.as_str())
                                .copied()
                                .ok_or_else(|| crate::error::evolution_source(&column.name))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    indices.sort_unstable();
                    indices.dedup();
                    affected.extend(
                        indices
                            .iter()
                            .copied()
                            .filter(|&index| index < source.fields().len()),
                    );
                    let input_schema = Arc::new(Schema::new_with_metadata(
                        indices
                            .iter()
                            .map(|index| fields[*index].clone())
                            .collect::<Vec<_>>(),
                        source.metadata().clone(),
                    ));
                    let planner = planner.get_or_insert_with(ExpressionPlanner::new);
                    let (expression, output) = DFSchema::try_from(input_schema.clone())
                        .and_then(|schema| planner.plan(expression.clone(), &schema))
                        .map_err(|error| crate::error::evolution_plan_error(name, error))?;
                    validation::validate_evolution_expression_type(
                        name,
                        output.data_type(),
                        data_type,
                    )?;
                    indices
                        .iter_mut()
                        .for_each(|index| *index = inputs.bind(*index));
                    let index = fields.len();
                    let field = Arc::new(Field::new(name, data_type.clone(), true));
                    steps.push(EvolutionStep::Add {
                        index,
                        field: field.clone(),
                        expression,
                        input: (input_schema, indices),
                    });
                    names.insert(name.as_str(), index);
                    fields.push(field);
                }
                ColumnEvolution::RemoveColumn { name } => {
                    let index = resolve()?;
                    names.remove(name.as_str());
                    if let Some(index) = inputs.lookup(index) {
                        steps.push(EvolutionStep::Remove(index));
                    }
                }
                ColumnEvolution::RenameColumn {
                    source_name,
                    target_name,
                } => {
                    let index = resolve()?;
                    validation::validate_evolution_target(
                        Some(index),
                        names.get(target_name.as_str()).copied(),
                    )?;
                    if source_name != target_name {
                        names.remove(source_name.as_str());
                        names.insert(target_name.as_str(), index);
                        fields[index] =
                            Arc::new(fields[index].as_ref().clone().with_name(target_name));
                    }
                }
                ColumnEvolution::ChangeDataType { data_type, .. } => {
                    let index = resolve()?;
                    let field = fields[index].as_ref();
                    if field.data_type() != data_type {
                        validation::validate_cast(field, data_type)?;
                        let field = Arc::new(field.clone().with_data_type(data_type.clone()));
                        fields[index] = field.clone();
                        steps.push(EvolutionStep::Cast {
                            index: inputs.bind(index),
                            field,
                        });
                    }
                }
            }
        }
        // Scan slots only once; removed or reused names no longer own their old slot.
        let (mut output_indices, fields): (Vec<_>, Vec<_>) = fields
            .into_iter()
            .enumerate()
            .filter(|(index, field)| names.get(field.name().as_str()) == Some(index))
            .unzip();
        let target_schema = Arc::new(Schema::new_with_metadata(fields, source.metadata().clone()));
        let (target_schema, projection) = options.resolve_projection(&target_schema)?;
        if let Some(projection) = projection {
            output_indices = projection
                .into_iter()
                .map(|index| output_indices[index])
                .collect();
        }
        output_indices
            .iter_mut()
            .for_each(|index| *index = inputs.bind(*index));
        let affected = affected
            .into_iter()
            .filter_map(|index| inputs.lookup(index))
            .collect();
        let source_schema = match inputs.columns {
            Some(columns) => Arc::new(
                source
                    .project(&columns.into_iter().collect::<Vec<_>>())
                    .map_err(crate::error::evolution_batch)?,
            ),
            None => source.clone(),
        };
        let lifetimes = crate::value_lifetime::ValueLifetimePlan::new(
            steps.iter().map(|step| match step {
                EvolutionStep::Add { index, input, .. } => (Some(*index), input.1.as_slice()),
                EvolutionStep::Cast { index, .. } => (Some(*index), std::slice::from_ref(index)),
                EvolutionStep::Remove(_) => (None, &[][..]),
            }),
            &output_indices,
        );
        Ok(Self {
            lifetimes,
            source_schema,
            target_schema,
            affected,
            steps,
            output_indices,
        })
    }

    pub(super) fn source_columns(&self) -> Vec<String> {
        self.source_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }
}
