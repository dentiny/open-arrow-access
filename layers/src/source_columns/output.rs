use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatchOptions, StringArray, UInt64Array};
use arrow_schema::Schema;
use open_arrow_access_core::{
    BytePosition, ObjectRef, ReadOptions, RecordBatch, ResolvedProjection, Result, SchemaRef,
};

use super::SourceColumn;

#[derive(Clone)]
pub(super) struct SourceOutput {
    input: SchemaRef,
    columns: Vec<(SourceColumn, String)>,
    projection: ResolvedProjection,
}
impl SourceOutput {
    pub(super) fn new(
        input: &SchemaRef,
        columns: &[(SourceColumn, String)],
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        let mut fields = input
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        fields.extend(
            columns
                .iter()
                .map(|(column, name)| column.field().with_name(name)),
        );
        crate::validation::validate_source_column_names(&fields)?;
        let schema = Arc::new(Schema::new_with_metadata(fields, input.metadata().clone()));
        let projection = ReadOptions {
            projection,
            ..Default::default()
        }
        .resolve_projection(&schema)?;
        Ok(Self {
            input: input.clone(),
            columns: columns.to_vec(),
            projection,
        })
    }
    pub(super) fn schema(&self) -> SchemaRef {
        self.projection.schema.clone()
    }
    pub(super) fn batch(
        &self,
        batch: RecordBatch,
        object: Option<&ObjectRef>,
        positions: &[BytePosition],
    ) -> Result<RecordBatch> {
        crate::validation::validate_source_schema(&batch.schema(), &self.input)?;
        let mut arrays = batch.columns().to_vec();
        for (column, _) in &self.columns {
            let array: ArrayRef = match column {
                SourceColumn::Object => {
                    let object = crate::validation::validate_source_object(object)?;
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                        object.locator(),
                        batch.num_rows(),
                    )))
                }
                SourceColumn::Offset => Arc::new(UInt64Array::from_iter_values(
                    positions.iter().map(|p| p.offset()),
                )),
                SourceColumn::OffsetInFrame => Arc::new(UInt64Array::from_iter_values(
                    positions.iter().map(|p| p.offset_in_frame()),
                )),
            };
            arrays.push(array);
        }
        let arrays = match &self.projection.indices {
            Some(indices) => indices.iter().map(|&i| arrays[i].clone()).collect(),
            None => arrays,
        };
        RecordBatch::try_new_with_options(
            self.schema(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(crate::error::source_column_batch_error)
    }
}
