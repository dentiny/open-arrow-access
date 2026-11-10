use super::{decode_error, input::ParquetInput};
use futures::TryStreamExt;
use open_arrow_access_core::{
    ByteReader, Capability, Error, ErrorKind, FactoryToken, FieldNameIndex, ReadRequest,
    ReaderFactory, RecordMetadata, RecordReader, Result, async_trait, record_stream_reader,
};
use parquet::arrow::{ProjectionMask, async_reader::ParquetRecordBatchStreamBuilder};

/// Arrow Parquet decoding over exact byte ranges with a known input length.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParquetFormat;

#[async_trait]
impl ReaderFactory for ParquetFormat {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    type Prepared = ();

    fn capability(&self, input: Capability) -> Capability {
        Capability {
            read: input.read && input.read_with_range,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn prepare(&self, _request: &ReadRequest) -> Result<()> {
        Ok(())
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        (): (),
    ) -> Result<Box<dyn RecordReader>> {
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
                let field_index = FieldNameIndex::new(stream.schema().fields());
                columns
                    .iter()
                    .map(|name| field_index.index_of(name))
                    .collect::<std::result::Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|e| decode_error("project", e))?
            .filter(|indices| indices.iter().copied().ne(0..indices.len()));
        // TODO: Decode row groups concurrently while preserving row order.
        Ok(Box::new(record_stream_reader(
            stream.map_err(|error| decode_error("decode_parquet", error)),
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            reorder,
        )) as Box<dyn RecordReader>)
    }
}
