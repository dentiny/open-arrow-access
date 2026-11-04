use std::{collections::HashSet, ops::Range, sync::Arc};

use futures::StreamExt;
use open_arrow_access_core::{
    Accessor, BoxFuture, ByteReader, Bytes, Error, ErrorKind, ObjectRef, ReadRequest, Reader,
    RecordBatch, RecordMetadata, RecordReader, Result,
};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::ArrowReaderOptions,
        async_reader::{
            AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
        },
    },
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};

/// Adapt any shared byte accessor into a single-file Arrow record accessor.
/// The input must expose range reads and its current byte length.
#[derive(Clone, Debug)]
pub struct ParquetAccessor<A> {
    inner: A,
}

impl<A> ParquetAccessor<A> {
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

    /// Open ranged byte input, inspect Parquet metadata and construct an ordered reader.
    /// Validate projection and retain the output schema before returning any batches.
    fn open(&self, request: ReadRequest) -> BoxFuture<'_, Result<Box<dyn RecordReader>>> {
        Box::pin(async move {
            let input = self.inner.open(request.object.clone()).await?;
            if !input.capabilities().range_read || input.metadata().content_length.is_none() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "open_parquet",
                    "Parquet requires range reads and a known byte length; materialize sequential input first",
                ));
            }
            let mut builder = ParquetRecordBatchStreamBuilder::new(ParquetInput { inner: input })
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
            Ok(Box::new(ParquetReader {
                stream,
                reorder,
                finished: false,
                metadata: RecordMetadata {
                    schema,
                    source: Some(request.object),
                },
            }) as Box<dyn RecordReader>)
        })
    }
}

struct ParquetInput {
    inner: Box<dyn ByteReader>,
}

impl AsyncFileReader for ParquetInput {
    /// Bridge a Parquet byte-range request to the backend-independent byte reader.
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move {
            self.inner
                .read_range(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        })
    }

    /// Fetch footer/schema metadata through range reads, honoring metadata/index options.
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let length = self
                .inner
                .metadata()
                .content_length
                .ok_or_else(|| ParquetError::General("input length is unknown".to_owned()))?;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(options.map(|o| o.metadata_options().clone()))
                .with_column_index_policy(
                    options.map_or(PageIndexPolicy::Skip, |o| o.column_index_policy()),
                )
                .with_offset_index_policy(
                    options.map_or(PageIndexPolicy::Skip, |o| o.offset_index_policy()),
                )
                .load_and_finish(self, length)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

struct ParquetReader {
    stream: ParquetRecordBatchStream<ParquetInput>,
    metadata: RecordMetadata,
    reorder: Option<Vec<usize>>,
    finished: bool,
}

impl Reader for ParquetReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    /// Borrow the fixed output schema and source selected during opening.
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    /// Decode the next ordered batch, apply field order and restore schema metadata.
    /// Stop reading permanently after EOF or the first decoding/projection failure.
    fn next(&mut self) -> BoxFuture<'_, Result<Option<RecordBatch>>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            let result = match self.stream.next().await {
                Some(Ok(batch)) => match self.reorder.as_ref() {
                    Some(indices) => batch
                        .project(indices)
                        .map(Some)
                        .map_err(|e| decode_error("project", e)),
                    None => Ok(Some(batch)),
                },
                Some(Err(error)) => Err(decode_error("decode_parquet", error)),
                None => Ok(None),
            };
            let result = result.and_then(|batch| {
                batch
                    .map(|batch| batch.with_schema(self.metadata.schema.clone()))
                    .transpose()
                    .map_err(|e| decode_error("restore_schema", e))
            });
            if !matches!(&result, Ok(Some(_))) {
                self.finished = true;
            }
            result
        })
    }
}

fn decode_error<E>(operation: &'static str, error: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::new(ErrorKind::Decode, operation, error.to_string()).with_source(error)
}
