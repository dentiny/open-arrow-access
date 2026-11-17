//! Shared provenance handling for file formats.
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{Schema, SchemaRef};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Error, ErrorKind, ObjectRef, ReadRequest, Reader, RecordBatch, RecordMetadata, RecordReader,
    Result, SourceColumn, StreamReader, into_stream,
};
use std::sync::Arc;

/// Request-side provenance support for FileAccessor. Native byte requests pass through.
pub trait FileRequest<R: Reader + ?Sized + 'static>: Send {
    fn take_sources(
        &mut self,
        supports: impl Fn(SourceColumn) -> bool,
    ) -> Result<Vec<SourceColumn>>;
    fn object(&self) -> &ObjectRef;
    fn wrap(reader: Box<R>, object: ObjectRef, columns: Vec<SourceColumn>) -> Result<Box<R>>;
}
impl FileRequest<dyn RecordReader> for ReadRequest {
    fn take_sources(
        &mut self,
        supports: impl Fn(SourceColumn) -> bool,
    ) -> Result<Vec<SourceColumn>> {
        let mut seen = std::collections::HashSet::new();
        for &column in &self.options.source_columns {
            if !seen.insert(column) {
                return Err(invalid("duplicate source column"));
            }
            if !supports(column) {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "prepare_source_columns",
                    format!("format does not support {}", column.name()),
                )
                .with_object(Some(&self.object)));
            }
        }
        let columns = self.options.source_columns.clone();
        if let Some(projection) = &mut self.options.projection {
            projection.retain(|name| !columns.iter().any(|c| c.name() == name));
        }
        Ok(columns)
    }
    fn object(&self) -> &ObjectRef {
        &self.object
    }
    fn wrap(
        reader: Box<dyn RecordReader>,
        object: ObjectRef,
        columns: Vec<SourceColumn>,
    ) -> Result<Box<dyn RecordReader>> {
        wrap(reader, object, columns)
    }
}
impl<R: Reader + ?Sized + 'static> FileRequest<R> for open_arrow_access_core::ByteReadRequest {
    fn take_sources(&mut self, _: impl Fn(SourceColumn) -> bool) -> Result<Vec<SourceColumn>> {
        Ok(vec![])
    }
    fn object(&self) -> &ObjectRef {
        &self.object
    }
    fn wrap(reader: Box<R>, _: ObjectRef, _: Vec<SourceColumn>) -> Result<Box<R>> {
        Ok(reader)
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
