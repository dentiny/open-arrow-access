use crate::{FileAccessor, scan::FileScanPlanner};
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{Schema, SchemaRef};
use futures::{FutureExt, StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Error, ErrorKind, ObjectRef, ReadRequest, ReaderFactory,
    ReaderFactoryExt, RecordBatch, RecordMetadata, RecordReader, Result, ScanPlan, SourceColumn,
    SourceColumnAccessor, SourceColumnPlanner, StreamReader, async_trait, into_stream,
};
use std::sync::Arc;

#[async_trait]
impl<B, F> SourceColumnAccessor for FileAccessor<B, F>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
{
    async fn open_source_columns(
        &self,
        request: ReadRequest,
        columns: &[SourceColumn],
    ) -> Result<Box<dyn RecordReader>> {
        let object = request.object.clone();
        let reader = self
            .binding
            .factory()
            .open_source_reader(self.binding.storage(), request, columns)
            .await?;
        wrap(reader, object.clone(), columns.to_vec()).map_err(|e| e.with_object(Some(&object)))
    }
}
#[async_trait]
impl<B, F> SourceColumnPlanner for FileAccessor<B, F>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
    Self: FileScanPlanner,
{
    async fn plan_source_columns(
        &self,
        request: ReadRequest,
        columns: &[SourceColumn],
    ) -> Result<ScanPlan> {
        let object = request.object.clone();
        let mut plan = self
            .plan_file_scan_with_source_columns(request, columns)
            .await
            .map_err(|e| e.with_object(Some(&object)))?;
        plan.metadata.schema = schema(&plan.metadata.schema, columns)?;
        let columns = columns.to_vec();
        let discovery_object = object.clone();
        plan.partitions = plan
            .partitions
            .map_err(move |error| error.with_object(Some(&discovery_object)))
            .map_ok(move |partition| {
                let object = object.clone();
                let columns = columns.clone();
                async move {
                    let reader = partition.await.map_err(|e| e.with_object(Some(&object)))?;
                    wrap(reader, object.clone(), columns).map_err(|e| e.with_object(Some(&object)))
                }
                .boxed()
            })
            .boxed();
        Ok(plan)
    }
}

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "source_columns", message)
}
pub(crate) fn schema(input: &SchemaRef, columns: &[SourceColumn]) -> Result<SchemaRef> {
    if columns.is_empty() {
        return Ok(input.clone());
    }
    if columns.contains(&SourceColumn::Object)
        && input.index_of(SourceColumn::Object.name()).is_ok()
    {
        return Err(invalid("source object column conflicts with data field"));
    }
    let mut fields = input
        .fields()
        .iter()
        .filter(|f| {
            !columns
                .iter()
                .any(|c| *c != SourceColumn::Object && c.name() == f.name())
        })
        .cloned()
        .collect::<Vec<_>>();
    fields.extend(columns.iter().map(|c| Arc::new(c.field())));
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input.metadata().clone(),
    )))
}
pub(crate) fn wrap(
    reader: Box<dyn RecordReader>,
    object: ObjectRef,
    columns: Vec<SourceColumn>,
) -> Result<Box<dyn RecordReader>> {
    if columns.is_empty() {
        return Ok(reader);
    }
    let output = schema(&reader.metadata().schema, &columns)?;
    let metadata = RecordMetadata {
        schema: output.clone(),
        ..reader.metadata().clone()
    };
    let ordering = reader.capability().ordering;
    let batches = into_stream(reader).and_then(move |batch| {
        let result = (|| {
            let arrays = output
                .fields()
                .iter()
                .map(|field| -> Result<ArrayRef> {
                    if field.name() == SourceColumn::Object.name()
                        && columns.contains(&SourceColumn::Object)
                    {
                        Ok(Arc::new(StringArray::from_iter_values(
                            std::iter::repeat_n(object.locator(), batch.num_rows()),
                        )))
                    } else {
                        Ok(batch
                            .column_by_name(field.name())
                            .ok_or_else(|| invalid("missing source field"))?
                            .clone())
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            RecordBatch::try_new(output.clone(), arrays).map_err(|e| invalid(e.to_string()))
        })();
        futures::future::ready(result)
    });
    Ok(Box::new(StreamReader::records(batches, metadata, ordering)))
}
