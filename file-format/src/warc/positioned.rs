use futures::{io::AsyncBufReadExt, stream};
use open_arrow_access_core::{
    Accessor, BytePosition, ByteReadRequest, ByteReader, Capability, Error, ErrorKind,
    FactoryToken, PositionedAsyncRead, PositionedBatch, PositionedRecordReader, ReadRequest,
    ReaderFactory, RecordCapability, RecordMetadata, Result, SchemaRef, StreamReader, async_trait,
    into_positioned_async_read,
};
use std::num::NonZeroUsize;

use super::{WarcArrowFormat, arrow::body_view_schema, batch::WarcBatchBuilder, decode_error};

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

/// Positioned WARC records whose `body: BinaryView` adopts the parser's body buffers.
/// All other fields, row coordinates, and validation match `PositionedWarcArrowFormat`.
#[derive(Clone, Copy, Debug)]
pub struct PositionedWarcArrowViewFormat {
    max_body_bytes: NonZeroUsize,
}

impl Default for PositionedWarcArrowViewFormat {
    fn default() -> Self {
        Self {
            max_body_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
        }
    }
}

impl PositionedWarcArrowViewFormat {
    /// Soft body-byte budget per batch, in addition to ReadOptions.batch_size.
    /// Defaults to 16 MiB. One oversized record is emitted alone, never split.
    pub fn with_max_body_bytes(mut self, bytes: NonZeroUsize) -> Self {
        self.max_body_bytes = bytes;
        self
    }
}

impl PositionedWarcArrowFormat {
    /// Avoid copying owned body bytes into a contiguous LargeBinary array.
    /// This opt-in changes only the body field's Arrow type to BinaryView.
    pub fn with_body_view(self) -> PositionedWarcArrowViewFormat {
        PositionedWarcArrowViewFormat::default()
    }
}

macro_rules! positioned_format {
    ($format:ty, $schema:expr, $body_limit:expr) => {
        #[async_trait]
        impl ReaderFactory for $format {
            type Request = ReadRequest;
            type Prepared = SchemaRef;
            type Reader = dyn PositionedRecordReader;
            fn capability(&self, input: Capability) -> RecordCapability {
                WarcArrowFormat.capability(input)
            }
            fn prepare(&self, request: &ReadRequest) -> Result<SchemaRef> {
                request
                    .options
                    .resolve_projection(&$schema)
                    .map(|projection| projection.schema)
            }
            async fn metadata(
                &self,
                _storage: &dyn Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
                request: ReadRequest,
            ) -> Result<RecordMetadata> {
                Ok(RecordMetadata {
                    schema: self.prepare(&request)?,
                    source: Some(request.object),
                })
            }
            async fn create_reader(
                &self,
                _token: &FactoryToken,
                input: Box<dyn ByteReader>,
                request: ReadRequest,
                schema: SchemaRef,
            ) -> Result<Box<dyn PositionedRecordReader>> {
                create_reader(input, request, schema, ($body_limit)(self)).await
            }
        }
    };
}
positioned_format!(
    PositionedWarcArrowFormat,
    super::arrow::arrow_schema(),
    |_| None
);
positioned_format!(
    PositionedWarcArrowViewFormat,
    body_view_schema(),
    |format: &PositionedWarcArrowViewFormat| Some(format.max_body_bytes.get())
);

async fn create_reader(
    input: Box<dyn ByteReader>,
    request: ReadRequest,
    schema: SchemaRef,
    max_body_bytes: Option<usize>,
) -> Result<Box<dyn PositionedRecordReader>> {
    let output = schema.clone();
    let source_uri = request.object.locator().to_owned();
    let batch_size = request.options.batch_size.get();
    let input = into_positioned_async_read(input, "read_warc");
    let batches = stream::try_unfold(input, move |mut input| {
        let output = output.clone();
        let source_uri = source_uri.clone();
        async move {
            let mut batch = WarcBatchBuilder::new(output, batch_size)?;
            let mut positions = Vec::new();
            while batch.len() < batch_size
                && max_body_bytes.is_none_or(|limit| batch.body_bytes() < limit)
            {
                let Some((headers, body, position)) = read_record(&mut input).await? else {
                    break;
                };
                batch.append(headers, body, &source_uri)?;
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
    Ok(reader)
}

async fn read_record(
    input: &mut PositionedAsyncRead,
) -> Result<Option<(warc::RawRecordHeader, Vec<u8>, BytePosition)>> {
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
    let (headers, body) = warc::WarcReader::new(input)
        .iter_raw_records()
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
    Ok(Some((headers, body, position)))
}
