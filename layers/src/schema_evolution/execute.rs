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
                } => {
                    let array = expression.evaluate(
                        &batch,
                        &values,
                        crate::error::evolution_batch,
                        |error| crate::error::evolution_expression_error(field.name(), error),
                    )?;
                    validation::validate_added_column(
                        field.name(),
                        array.as_ref(),
                        field.data_type(),
                        batch.num_rows(),
                    )?;
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
