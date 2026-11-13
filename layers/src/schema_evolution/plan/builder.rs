use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use open_arrow_access_core::{ReadOptions, Result};

use super::{EvolutionStep, SchemaEvolutionPlan, SourceProjection};
use crate::{
    ColumnEvolution, ColumnExpression,
    expression::{ExpressionPlanner, PlannedExpression},
    validation,
    value_lifetime::ValueLifetimePlan,
};

/// Mutable namespace and execution steps for one ordered schema transformation.
pub(super) struct EvolutionPlanner<'a> {
    source: &'a SchemaRef,
    names: HashMap<String, usize>,
    fields: Vec<FieldRef>,
    inputs: SourceProjection,
    affected: HashSet<usize>,
    steps: Vec<EvolutionStep>,
    expressions: Option<ExpressionPlanner>,
}

impl<'a> EvolutionPlanner<'a> {
    pub(super) fn new(source: &'a SchemaRef, project_source: bool) -> Result<Self> {
        let names: HashMap<_, _> = source
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| (field.name().clone(), index))
            .collect();
        validation::validate_evolution_source(names.len(), source.fields().len())?;
        Ok(Self {
            source,
            names,
            fields: source.fields().to_vec(),
            inputs: SourceProjection::new(source.fields().len(), project_source),
            affected: HashSet::new(),
            steps: Vec::new(),
            expressions: None,
        })
    }

    pub(super) fn apply(&mut self, change: &ColumnEvolution) -> Result<()> {
        match change {
            ColumnEvolution::AddColumn {
                name,
                data_type,
                expression,
            } => self.add(name, data_type.as_ref(), expression),
            ColumnEvolution::RemoveColumn { name } => self.remove(name),
            ColumnEvolution::RenameColumn {
                source_name,
                target_name,
            } => self.rename(source_name, target_name),
            ColumnEvolution::ChangeDataType { name, data_type } => self.cast(name, data_type),
        }
    }

    fn resolve(&mut self, name: &str) -> Result<usize> {
        let index = self
            .names
            .get(name)
            .copied()
            .ok_or_else(|| crate::error::evolution_source(name))?;
        if index < self.source.fields().len() {
            self.affected.insert(index);
        }
        Ok(index)
    }

    fn plan_expression(
        &mut self,
        expression: &ColumnExpression,
    ) -> datafusion::error::Result<PlannedExpression> {
        let planner = self.expressions.get_or_insert_with(ExpressionPlanner::new);
        match expression {
            ColumnExpression::Sql(sql) => planner.plan_sql(sql, &self.fields, &self.names),
            ColumnExpression::Expr(expression) => planner.plan_expr(
                expression,
                &self.fields,
                &self.names,
                self.source.metadata(),
            ),
        }
    }

    fn add(
        &mut self,
        name: &str,
        data_type: Option<&DataType>,
        expression: &ColumnExpression,
    ) -> Result<()> {
        validation::validate_evolution_target(None, self.names.get(name).copied())?;
        let PlannedExpression {
            expression: physical,
            output,
            input: (input_schema, mut indices),
        } = self
            .plan_expression(expression)
            .map_err(|error| crate::error::evolution_plan_error(name, error))?;
        self.affected.extend(
            indices
                .iter()
                .copied()
                .filter(|&index| index < self.source.fields().len()),
        );
        let field = Arc::new(added_field(name, data_type, expression, &output)?);
        indices
            .iter_mut()
            .for_each(|index| *index = self.inputs.bind(*index));
        let index = self.fields.len();
        self.steps.push(EvolutionStep::Add {
            index,
            field: field.clone(),
            expression: physical,
            input: (input_schema, indices),
        });
        self.names.insert(name.to_owned(), index);
        self.fields.push(field);
        Ok(())
    }

    fn remove(&mut self, name: &str) -> Result<()> {
        let index = self.resolve(name)?;
        self.names.remove(name);
        if let Some(index) = self.inputs.lookup(index) {
            self.steps.push(EvolutionStep::Remove(index));
        }
        Ok(())
    }

    fn rename(&mut self, source_name: &str, target_name: &str) -> Result<()> {
        let index = self.resolve(source_name)?;
        validation::validate_evolution_target(Some(index), self.names.get(target_name).copied())?;
        if source_name != target_name {
            self.names.remove(source_name);
            self.names.insert(target_name.to_owned(), index);
            self.fields[index] =
                Arc::new(self.fields[index].as_ref().clone().with_name(target_name));
        }
        Ok(())
    }

    fn cast(&mut self, name: &str, data_type: &DataType) -> Result<()> {
        let index = self.resolve(name)?;
        let field = self.fields[index].as_ref();
        if field.data_type() != data_type {
            validation::validate_cast(field, data_type)?;
            let field = Arc::new(field.clone().with_data_type(data_type.clone()));
            self.fields[index] = field.clone();
            self.steps.push(EvolutionStep::Cast {
                index: self.inputs.bind(index),
                field,
            });
        }
        Ok(())
    }

    pub(super) fn finish(mut self, options: &ReadOptions) -> Result<SchemaEvolutionPlan> {
        // Scan slots only once; removed or reused names no longer own their old slot.
        let (mut output_indices, fields): (Vec<_>, Vec<_>) = self
            .fields
            .into_iter()
            .enumerate()
            .filter(|(index, field)| self.names.get(field.name()) == Some(index))
            .unzip();
        let target_schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.source.metadata().clone(),
        ));
        let (target_schema, projection) = options.resolve_projection(&target_schema)?;
        if let Some(projection) = projection {
            output_indices = projection
                .into_iter()
                .map(|index| output_indices[index])
                .collect();
        }
        output_indices
            .iter_mut()
            .for_each(|index| *index = self.inputs.bind(*index));
        let affected = self
            .affected
            .into_iter()
            .filter_map(|index| self.inputs.lookup(index))
            .collect();
        let source_schema = self.inputs.into_schema(self.source)?;
        let lifetimes = ValueLifetimePlan::new(
            self.steps.iter().map(|step| match step {
                EvolutionStep::Add { index, input, .. } => (Some(*index), input.1.as_slice()),
                EvolutionStep::Cast { index, .. } => (Some(*index), std::slice::from_ref(index)),
                EvolutionStep::Remove(_) => (None, &[][..]),
            }),
            &output_indices,
        );
        Ok(SchemaEvolutionPlan {
            source_schema,
            target_schema,
            affected,
            steps: self.steps,
            output_indices,
            lifetimes,
        })
    }
}

fn added_field(
    name: &str,
    data_type: Option<&DataType>,
    expression: &ColumnExpression,
    output: &Field,
) -> Result<Field> {
    let mut field = if let Some(data_type) = data_type {
        validation::validate_evolution_expression_type(name, output.data_type(), data_type)?;
        Field::new(name, data_type.clone(), true)
    } else {
        validation::validate_evolution_type(output.data_type(), false)
            .map_err(|error| crate::error::evolution_config_error(name, error))?;
        output.clone().with_name(name)
    };
    if let ColumnExpression::Sql(sql) = expression {
        field
            .metadata_mut()
            .insert("open_arrow_access:generated_expression".into(), sql.clone());
    }
    Ok(field)
}
