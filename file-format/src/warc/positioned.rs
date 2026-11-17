use futures::{io::AsyncBufReadExt, stream};
use open_arrow_access_core::{
    BytePosition, ByteReader, Capability, Error, ErrorKind, FactoryToken, PositionedAsyncRead,
    PositionedBatch, PositionedRecordReader, ReadRequest, ReaderFactory, RecordCapability,
    RecordMetadata, Result, SchemaRef, StreamReader, async_trait, into_positioned_async_read,
};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};

use super::{
    WarcArrowFormat, WarcRecord,
    arrow::{append_record, date_type, new_batch, raw_projection},
    decode_error,
};

/// Decode WARC directly into Arrow batches paired with original row coordinates.
///
/// Select with `WarcArrowFormat.with_source_positions()`. For compressed storage,
/// install `SourcePositionLayer` before this format. Each record is parsed once;
/// batch size follows `ReadOptions.batch_size`. No reserialization, byte threshold,
/// or independently reopened per-record decoder is involved.
#[derive(Clone, Copy, Debug, Default)]
pub struct PositionedWarcArrowFormat;

impl WarcArrowFormat {
    /// Select a decoder that returns Arrow batches paired with row coordinates.
    ///
    /// ```
    /// use open_arrow_access_core::{ByteAccessor, FactoryAccessor};
    /// use open_arrow_access_file_format::warc::WarcArrowFormat;
    /// use open_arrow_access_layers::SourceColumns;
    ///
    /// # fn source(storage: impl ByteAccessor + 'static) {
    /// let decoded = FactoryAccessor::new(storage, WarcArrowFormat.with_source_positions());
    /// let source = SourceColumns::new()
    ///     .with_object("source_path")
    ///     .with_offset("record_offset")
    ///     .wrap(decoded);
    /// # }
    /// ```
    pub fn with_source_positions(self) -> PositionedWarcArrowFormat {
        PositionedWarcArrowFormat
    }
}

#[async_trait]
impl ReaderFactory for PositionedWarcArrowFormat {
    type Request = ReadRequest;
    type Prepared = SchemaRef;
    type Reader = dyn PositionedRecordReader;

    fn capability(&self, input: Capability) -> RecordCapability {
        WarcArrowFormat.capability(input)
    }
    fn prepare(&self, request: &ReadRequest) -> Result<SchemaRef> {
        WarcArrowFormat.prepare(request)
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<Box<dyn PositionedRecordReader>> {
        let schema = raw_projection(&schema)?;
        let output = schema.clone();
        let source_uri = request.object.locator().to_owned();
        let batch_size = request.options.batch_size.get();
        let input = into_positioned_async_read(input, "read_warc");
        let batches = stream::try_unfold(input, move |mut input| {
            let output = output.clone();
            let source_uri = source_uri.clone();
            async move {
                let mut batch = new_batch(&output)?;
                let mut positions = Vec::new();
                while batch.len() < batch_size {
                    let Some((record, position)) = read_record(&mut input).await? else {
                        break;
                    };
                    append_record(&mut batch, &record, &source_uri)?;
                    positions.push(position);
                }
                if batch.is_empty() {
                    return Ok(None);
                }
                Ok(Some((
                    PositionedBatch::try_new(batch.finish()?, positions)?,
                    input,
                )))
            }
        });
        let reader: Box<dyn PositionedRecordReader> = Box::new(StreamReader::records(
            batches,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            open_arrow_access_core::OrderRequirement::Reorderable,
        ));
        if reader.metadata().schema.index_of("date").is_err() {
            return Ok(reader);
        }
        SchemaEvolutionLayer::try_new(vec![ColumnEvolution::change_data_type("date", date_type())])?
            .wrap_positioned_reader(reader)
    }
}

async fn read_record(
    input: &mut PositionedAsyncRead,
) -> Result<Option<(WarcRecord, BytePosition)>> {
    // Consume inter-record whitespace before marking, so coordinates identify the
    // record itself. No buffering adapter may read ahead of the position observer.
    loop {
        let available = input.fill_buf().await.map_err(decode_error)?;
        if available.is_empty() {
            return Ok(None);
        }
        let whitespace = available
            .iter()
            .take_while(|b| b.is_ascii_whitespace())
            .count();
        if whitespace == 0 {
            break;
        }
        input.consume_unpin(whitespace);
    }
    let observer = input.observer();
    observer.mark();
    let record = warc::WarcReader::new(input)
        .iter_records()
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::Decode, "decode_warc", "missing WARC record"))?
        .map_err(decode_error)?;
    let position = observer.marked_position().ok_or_else(|| {
        Error::new(
            ErrorKind::Unsupported,
            "decode_warc",
            "WARC input has no byte positions",
        )
    })?;
    Ok(Some((record, position)))
}
