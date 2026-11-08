use arrow_array::{ArrayRef, RecordBatchOptions};
use serde::Serialize;

use crate::{Error, ErrorKind, ReadOptions, RecordBatch, Result, SchemaRef};

/// Encode Serde records into one batch with an explicit Arrow schema.
///
/// Projection selects top-level fields by name in caller order. Only selected
/// fields are encoded; validate source records before calling `push` when omitted
/// fields must also be checked. Binary fields accept byte sequences directly.
/// Schema and field metadata are preserved, including for zero-column batches.
///
/// Callers control batch size. An append error invalidates the builder; `finish`
/// cannot return a partial batch. Finishing consumes the builder.
///
/// ```
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use open_arrow_access_core::RecordBatchBuilder;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Record { id: u64, data: Vec<u8> }
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::UInt64, false),
///     Field::new("data", DataType::LargeBinary, false),
/// ]));
/// let mut builder = RecordBatchBuilder::new(schema, None)?;
/// builder.push(&Record { id: 1, data: vec![0, 255] })?;
/// let batch = builder.finish()?;
/// assert_eq!(batch.num_rows(), 1);
/// # Ok::<(), open_arrow_access_core::Error>(())
/// ```
pub struct RecordBatchBuilder {
    schema: SchemaRef,
    builder: Option<serde_arrow::ArrayBuilder>,
    rows: usize,
}

impl RecordBatchBuilder {
    /// Resolve projection and validate the selected schema before appending rows.
    pub fn new(schema: SchemaRef, projection: Option<Vec<String>>) -> Result<Self> {
        let (schema, _) = ReadOptions {
            projection,
            ..Default::default()
        }
        .resolve_projection(&schema)?;
        // Use Marrow's additive Arrow interop instead of serde_arrow's version-
        // selected API, which changes when another crate enables a newer Arrow.
        let fields = schema
            .fields()
            .iter()
            .map(|field| marrow::datatypes::Field::try_from(field.as_ref()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| map_error(ErrorKind::InvalidInput, error))?;
        let builder = serde_arrow::ArrayBuilder::from_marrow(&fields)
            .map_err(|error| map_error(ErrorKind::InvalidInput, error))?;
        Ok(Self {
            schema,
            builder: Some(builder),
            rows: 0,
        })
    }

    /// Borrow the fixed, projected output schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Number of successfully appended rows.
    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append a struct or map record. An error permanently invalidates the builder.
    pub fn push<T: Serialize + ?Sized>(&mut self, record: &T) -> Result<()> {
        let mut builder = self.builder.take().ok_or_else(failed_builder)?;
        builder
            .push(record)
            .map_err(|error| map_error(ErrorKind::Decode, error))?;
        self.builder = Some(builder);
        self.rows += 1;
        Ok(())
    }

    /// Consume the builder, preserving row count even when no columns are selected.
    pub fn finish(self) -> Result<RecordBatch> {
        let arrays = self
            .builder
            .ok_or_else(failed_builder)?
            .into_marrow()
            .map_err(|error| map_error(ErrorKind::Decode, error))?;
        let columns = arrays
            .into_iter()
            .map(ArrayRef::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| map_error(ErrorKind::Decode, error))?;
        RecordBatch::try_new_with_options(
            self.schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(self.rows)),
        )
        .map_err(|error| map_error(ErrorKind::Decode, error))
    }
}

fn failed_builder() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "build_record_batch",
        "builder is invalid after an append error",
    )
}

fn map_error(kind: ErrorKind, error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(kind, "build_record_batch", error.to_string()).with_source(error)
}
