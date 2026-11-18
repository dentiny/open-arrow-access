use super::{
    decode_error, error::parquet_input_error, input::ParquetInput, validation::validate_input,
};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Capability, FactoryToken, FieldNameIndex,
    OrderRequirement, ReadRequest, ReaderFactory, RecordCapability, RecordMetadata, RecordReader,
    ResolvedProjection, Result, async_trait, record_stream_reader,
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

    fn capability(&self, input: Capability) -> RecordCapability {
        Capability {
            read: input.read && input.read_with_range,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    fn prepare(&self, _request: &ReadRequest) -> Result<()> {
        Ok(())
    }

    async fn metadata(
        &self,
        storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
        request: ReadRequest,
    ) -> Result<RecordMetadata> {
        let input = storage.open(ByteReadRequest::from(&request)).await?;
        validate_input(input.as_ref(), parquet_input_error)?;
        let builder = ParquetRecordBatchStreamBuilder::new(ParquetInput::new(input))
            .await
            .map_err(|e| decode_error("inspect_parquet", e))?;
        let schema = request.options.resolve_projection(builder.schema())?.schema;
        Ok(RecordMetadata {
            schema,
            source: Some(request.object),
        })
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        (): (),
    ) -> Result<Box<dyn RecordReader>> {
        validate_input(input.as_ref(), parquet_input_error)?;
        let builder = ParquetRecordBatchStreamBuilder::new(ParquetInput::new(input))
            .await
            .map_err(|e| decode_error("inspect_parquet", e))?;

        create_reader(builder, request, None)
    }
}

pub(super) fn create_reader(
    mut builder: ParquetRecordBatchStreamBuilder<ParquetInput>,
    request: ReadRequest,
    row_groups: Option<Vec<usize>>,
) -> Result<Box<dyn RecordReader>> {
    if let Some(predicate) = &request.options.pruning_predicate {
        let groups =
            row_groups.unwrap_or_else(|| (0..builder.metadata().num_row_groups()).collect());
        let groups = groups
            .into_iter()
            .filter(|index| {
                super::pruning::may_match(
                    predicate,
                    builder.schema(),
                    builder.metadata().row_group(*index),
                )
            })
            .collect();
        builder = builder.with_row_groups(groups);
    } else if let Some(groups) = row_groups {
        builder = builder.with_row_groups(groups);
    }
    // The native projection mask selects fields in file order. Reorder
    // decoded top-level fields separately to honor the caller's order.
    // The native stream drops schema metadata; retain it from inspection.
    let ResolvedProjection {
        schema,
        indices: projection,
    } = request.options.resolve_projection(builder.schema())?;
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
    Ok(Box::new(record_stream_reader(
        stream.map_err(|error| decode_error("decode_parquet", error)),
        RecordMetadata {
            schema,
            source: Some(request.object),
        },
        reorder,
        OrderRequirement::Reorderable,
    )) as Box<dyn RecordReader>)
}
