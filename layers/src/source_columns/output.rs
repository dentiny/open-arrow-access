use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatchOptions, StringArray, UInt64Array};
use arrow_schema::Schema;
use futures::stream;
use open_arrow_access_core::{
    BytePosition, ObjectRef, ReadOptions, RecordBatch, RecordCapability, RecordReader,
    ResolvedProjection, Result, SchemaRef, StreamReader, map_record_reader,
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
    fn batch(
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
    pub(super) fn object_reader(self, reader: Box<dyn RecordReader>) -> Box<dyn RecordReader> {
        let object = reader.metadata().source.clone();
        let capability = RecordCapability::sequential(reader.capability().ordering);
        Box::new(map_record_reader(
            reader,
            self.schema(),
            capability,
            move |batch| self.batch(batch, object.as_ref(), &[]),
        ))
    }
    pub(super) async fn unit_reader(
        self,
        mut reader: Box<dyn RecordReader>,
        positions: Vec<BytePosition>,
    ) -> Result<Box<dyn RecordReader>> {
        let mut metadata = reader.metadata().clone();
        crate::validation::validate_source_schema(&metadata.schema, &self.input)?;
        let ordering = reader.capability().ordering;
        let mut batches = Vec::new();
        let mut rows = 0;
        while let Some(batch) = reader.next().await? {
            let end = rows + batch.num_rows();
            crate::validation::validate_source_row_bound(end, positions.len())?;
            batches.push(self.batch(batch, metadata.source.as_ref(), &positions[rows..end])?);
            rows = end;
        }
        crate::validation::validate_source_row_count(rows, positions.len())?;
        metadata.schema = self.schema();
        Ok(Box::new(StreamReader::records(
            stream::iter(batches.into_iter().map(Ok)),
            metadata,
            ordering,
        )))
    }
}
