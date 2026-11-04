use std::{collections::HashSet, sync::Arc};

use open_arrow_access_core::{
    Accessor, BoxFuture, ByteReader, Capability, Error, ErrorKind, ObjectRef, ReadRequest,
    RecordMetadata, RecordReader, Result,
};
use parquet::arrow::{ProjectionMask, async_reader::ParquetRecordBatchStreamBuilder};

use super::{decode_error, input::ParquetInput, reader::ParquetReader};

/// Adapt any shared byte accessor into a single-file Arrow record accessor.
/// The input must expose range reads and its current byte length.
#[derive(Clone, Debug)]
pub struct ParquetAccessor<A> {
    inner: A,
}

impl<A> ParquetAccessor<A> {
    /// Build a single-file Parquet adapter over the provided byte accessor.
    pub fn new(inner: A) -> Self {
        Self { inner }
    }
}

impl<A> Accessor for ParquetAccessor<A>
where
    A: Accessor<Request = ObjectRef, Reader = dyn ByteReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    /// Expose record reading when the wrapped byte accessor supports exact ranges.
    fn capability(&self) -> Capability {
        let input = self.inner.capability();
        Capability {
            read: input.read && input.read_with_range,
            ..Capability::default()
        }
    }

    /// Open ranged byte input, inspect Parquet metadata and construct an ordered reader.
    /// Validate projection and retain the output schema before returning any batches.
    fn open(&self, request: ReadRequest) -> BoxFuture<'_, Result<Box<dyn RecordReader>>> {
        Box::pin(async move {
            let input = self.inner.open(request.object.clone()).await?;
            if !input.capability().read_with_range || input.metadata().content_length.is_none() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "open_parquet",
                    "Parquet requires range reads and a known byte length; materialize sequential input first",
                ));
            }
            let mut builder = ParquetRecordBatchStreamBuilder::new(ParquetInput::new(input))
                .await
                .map_err(|e| decode_error("inspect_parquet", e))?;

            // The native projection mask selects fields in file order. Reorder
            // decoded top-level fields separately to honor the caller's order.
            // The native stream drops schema metadata; retain it from inspection.
            let mut schema = builder.schema().clone();
            if let Some(columns) = request.options.projection.as_ref() {
                let mut seen = HashSet::new();
                let mut indices = Vec::with_capacity(columns.len());
                for name in columns {
                    if !seen.insert(name) {
                        return Err(Error::new(
                            ErrorKind::InvalidInput,
                            "project",
                            format!("duplicate field {name:?}"),
                        ));
                    }
                    indices.push(builder.schema().index_of(name).map_err(|e| {
                        Error::new(
                            ErrorKind::InvalidInput,
                            "project",
                            format!("unknown field {name:?}"),
                        )
                        .with_source(e)
                    })?);
                }
                schema = Arc::new(
                    schema
                        .project(&indices)
                        .map_err(|e| decode_error("project", e))?,
                );
                let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
                builder = builder.with_projection(mask);
            }
            let stream = builder
                .with_batch_size(request.options.batch_size.get())
                .build()
                .map_err(|e| decode_error("open_parquet", e))?;
            let reorder = request
                .options
                .projection
                .as_ref()
                .map(|columns| {
                    columns
                        .iter()
                        .map(|name| stream.schema().index_of(name))
                        .collect::<std::result::Result<Vec<_>, _>>()
                })
                .transpose()
                .map_err(|e| decode_error("project", e))?;
            Ok(Box::new(ParquetReader::new(
                stream,
                RecordMetadata {
                    schema,
                    source: Some(request.object),
                },
                reorder,
            )) as Box<dyn RecordReader>)
        })
    }
}
