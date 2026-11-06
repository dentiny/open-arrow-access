use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ReadRequest,
    RecordMetadata, RecordReader, Result, async_trait,
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
    pub fn new(inner: A) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<A> Accessor for ParquetAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        let input = self.inner.capability();
        Capability {
            read: input.read && input.read_with_range,
            ..Capability::default()
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let input = self
            .inner
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
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
        let (schema, projection) = request.options.resolve_projection(builder.schema())?;
        if let Some(indices) = projection {
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
            .map_err(|e| decode_error("project", e))?
            .filter(|indices| indices.iter().copied().ne(0..indices.len()));
        Ok(Box::new(ParquetReader::new(
            stream,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            reorder,
        )) as Box<dyn RecordReader>)
    }
}
