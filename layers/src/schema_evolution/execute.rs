use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, LargeStringArray, RecordBatch, cast::AsArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType};
use futures::stream;
use open_arrow_access_core::{RecordMetadata, RecordReader, Result, StreamReader};

use super::plan::{EvolutionStep, SchemaEvolutionPlan};
use crate::{batch::assemble_batch, validation};

impl SchemaEvolutionPlan {
    pub(super) fn into_reader(
        mut self,
        inner: Box<dyn RecordReader>,
    ) -> Result<Box<dyn RecordReader>> {
        let source = &inner.metadata().schema;
        validation::validate_evolution_input_schema(source, &self.source_schema)?;
        self.source_schema = source.clone();
        if self.steps.is_empty()
            && self.target_schema == *source
            && self
                .output_indices
                .iter()
                .copied()
                .eq(0..source.fields().len())
        {
            return Ok(inner);
        }
        let metadata = RecordMetadata {
            schema: Arc::clone(&self.target_schema),
            source: inner.metadata().source.clone(),
        };
        let capability = inner.capability();
        let batches = stream::try_unfold((inner, self), |(mut inner, plan)| async move {
            let Some(batch) = inner.next().await? else {
                return Ok(None);
            };
            Ok(Some((plan.convert(batch)?, (inner, plan))))
        });
        Ok(Box::new(
            StreamReader::records(batches, metadata, capability.ordering)
                .with_capability(capability),
        ))
    }

    fn convert(&self, batch: RecordBatch) -> Result<RecordBatch> {
        validation::validate_evolution_batch_schema(
            batch.schema_ref(),
            &self.source_schema,
            &self.affected,
        )?;
        let mut values = HashMap::new();
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        for (step, release) in self.steps.iter().zip(&self.lifetimes.releases) {
            match step {
                EvolutionStep::Add {
                    index,
                    field,
                    expression,
                    input,
                } => {
                    let view = assemble_batch(&input.0, &input.1, &batch, &values)
                        .map_err(crate::error::evolution_batch)?;
                    let array = expression
                        .evaluate(&view)
                        .and_then(|value| value.into_array(batch.num_rows()))
                        .map_err(|error| {
                            crate::error::evolution_expression_error(field.name(), error)
                        })?;
                    validation::validate_added_column(field, array.as_ref(), batch.num_rows())?;
                    values.insert(*index, array);
                }
                EvolutionStep::Cast { index, field } => {
                    let column = values.get(index).unwrap_or_else(|| batch.column(*index));
                    let target = field.data_type();
                    let array = cast_with_options(column.as_ref(), target, &options)
                        .or_else(|error| match target {
                            DataType::Timestamp(_, _) if column.data_type().is_string() => {
                                cast_minute_timestamps(column.as_ref(), target, &options)
                            }
                            _ => Err(error),
                        })
                        .map_err(|error| {
                            crate::error::cast_error(
                                field.name(),
                                column.data_type(),
                                target,
                                error,
                            )
                        })?;
                    values.insert(*index, array);
                }
                EvolutionStep::Remove(index) => {
                    values.remove(index);
                }
            }
            for index in release {
                values.remove(index);
            }
        }
        assemble_batch(&self.target_schema, &self.output_indices, &batch, &values)
            .map_err(crate::error::evolution_batch)
    }
}

/// Supply omitted seconds, then let Arrow validate dates, offsets, timezones and range.
fn cast_minute_timestamps(
    input: &dyn Array,
    target: &DataType,
    options: &CastOptions<'_>,
) -> std::result::Result<ArrayRef, ArrowError> {
    let strings = cast_with_options(input, &DataType::LargeUtf8, options)?;
    let normalized = LargeStringArray::from_iter(strings.as_string::<i64>().iter().map(|value| {
        value.map(|value| {
            if value.as_bytes().get(13) == Some(&b':')
                && matches!(
                    value.as_bytes().get(16),
                    None | Some(b'Z' | b'z' | b'+' | b'-')
                )
                && value.get(..16).is_some()
            {
                format!("{}:00{}", &value[..16], &value[16..])
            } else {
                value.to_owned()
            }
        })
    }));
    cast_with_options(&normalized, target, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnEvolution;
    use arrow_array::Int64Array;
    use arrow_schema::{Field, Schema};
    use open_arrow_access_core::ReadOptions;

    #[test]
    fn sql_branching_dependencies_keep_outputs_and_release_dead_values() {
        let batch =
            RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
                .unwrap();
        let changes = [
            ("a", "n + 1"),
            ("b", "a + 1"),
            ("dead", "100"),
            ("c", "a + b"),
            ("d", "c + 1"),
        ]
        .map(|(name, sql)| ColumnEvolution::add_column_sql(name, sql));
        let plan = SchemaEvolutionPlan::new(
            &changes,
            &batch.schema(),
            &ReadOptions {
                projection: Some(vec!["a".into(), "d".into()]),
                ..ReadOptions::default()
            },
            false,
        )
        .unwrap();
        assert_eq!(
            plan.lifetimes.releases,
            [vec![], vec![], vec![3], vec![2], vec![4]]
        );
        let output = plan.convert(batch).unwrap();
        assert_eq!(
            output.column(0).to_data(),
            Int64Array::from(vec![2]).to_data()
        );
        assert_eq!(
            output.column(1).to_data(),
            Int64Array::from(vec![6]).to_data()
        );
    }

    #[test]
    fn sql_chain_plan_is_compact_and_releases_intermediates() {
        const WIDTH: usize = 256;
        let source = Arc::new(Schema::new(
            (0..WIDTH)
                .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let changes = (0..WIDTH)
            .map(|i| {
                ColumnEvolution::add_column_sql(
                    format!("g{i}"),
                    format!(
                        "{} + 1",
                        if i == 0 {
                            format!("c{}", WIDTH - 1)
                        } else {
                            format!("g{}", i - 1)
                        }
                    ),
                )
            })
            .collect::<Vec<_>>();
        let plan = SchemaEvolutionPlan::new(
            &changes,
            &source,
            &ReadOptions {
                projection: Some(vec![format!("g{}", WIDTH - 1)]),
                ..ReadOptions::default()
            },
            false,
        )
        .unwrap();
        for (i, step) in plan.steps.iter().enumerate() {
            let EvolutionStep::Add { index, input, .. } = step else {
                panic!("expected add")
            };
            assert_eq!(input.0.fields().len(), 1);
            assert_eq!(input.1, [WIDTH - 1 + i]);
            assert_eq!(*index, WIDTH + i);
            assert_eq!(
                plan.lifetimes.releases[i],
                if i == 0 { vec![] } else { vec![WIDTH + i - 1] }
            );
        }
        assert_eq!(plan.output_indices, [WIDTH * 2 - 1]);
        assert_eq!(plan.target_schema.fields().len(), 1);
    }
}
