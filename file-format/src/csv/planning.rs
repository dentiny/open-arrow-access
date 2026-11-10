use futures::{
    StreamExt, TryStreamExt,
    io::{AsyncReadExt, Cursor},
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt,
    RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, into_async_read,
};

use super::{CsvAccessor, CsvDialect, error};
use crate::scan::input::ChunkInput;

#[async_trait]
impl<B> ScanPlanner for CsvAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory().clone();
        let Some(dialect) = format.parallel_dialect() else {
            return Ok(ScanPlan::whole_input(self.open(request).await?));
        };
        let (schema, _) = format.prepare(&request)?;
        let metadata = RecordMetadata {
            schema,
            source: Some(request.object.clone()),
        };
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        // Workers read canonical CSV, regardless of the source dialect.
        let format = format.with_dialect(CsvDialect {
            has_header: dialect.has_header,
            validate_header: dialect.validate_header,
            allow_truncated_rows: dialect.allow_truncated_rows,
            ..CsvDialect::default()
        });
        let target_bytes = self.partition_options.target_bytes.get();
        // Initialization runs on first poll, keeping partition discovery lazy.
        let partitions = stream::once(async move {
            let frames = CsvChunks::new(input, dialect).await?;
            Ok(stream::try_unfold(frames, move |mut frames| {
                let format = format.clone();
                let request = request.clone();
                async move {
                    let Some(bytes) = frames
                        .next_chunk(request.options.batch_size.get(), target_bytes)
                        .await?
                    else {
                        return Ok(None);
                    };
                    let partition: ScanPartition =
                        Box::pin(
                            async move { format.open_reader(&ChunkInput(bytes), request).await },
                        );
                    Ok(Some((partition, frames)))
                }
            }))
        })
        .try_flatten();
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

struct CsvChunks {
    records: BoxStream<'static, Result<csv_async::ByteRecord>>,
    header: Option<csv_async::ByteRecord>,
    // Validate a header-only input once even when there are no data records.
    emitted: bool,
}

impl CsvChunks {
    async fn new(input: Box<dyn ByteReader>, dialect: CsvDialect) -> Result<Self> {
        let mut input = into_async_read(input, "frame_csv");
        // csv-async's parser does not recognize a BOM split across input chunks.
        // Normalize only the source prefix, stopping as soon as a BOM is ruled out.
        let mut prefix = Vec::new();
        while prefix.len() < 3 && b"\xef\xbb\xbf".starts_with(&prefix) {
            let mut byte = [0];
            if input.read(&mut byte).await.map_err(error::input_error)? == 0 {
                break;
            }
            prefix.push(byte[0]);
        }
        if prefix == b"\xef\xbb\xbf" {
            prefix.clear();
        }
        // A skipped blank line disables the parser's own source-start BOM removal.
        prefix.insert(0, dialect.terminator.unwrap_or(b'\n'));
        let mut records = dialect
            .reader_builder()
            .create_reader(Cursor::new(prefix).chain(input))
            .into_byte_records()
            .map_err(error::record_error)
            .boxed();
        let header = if dialect.has_header {
            records.try_next().await?
        } else {
            None
        };
        Ok(Self {
            records,
            header,
            emitted: false,
        })
    }

    async fn next_chunk(&mut self, row_target: usize, byte_target: usize) -> Result<Option<Bytes>> {
        let mut output = Vec::new();
        let mut rows = 0;
        {
            // Quoting every field preserves data BOMs and arbitrary field bytes.
            // Leave row-length and UTF-8 validation to the Arrow worker.
            let mut writer = csv::WriterBuilder::new()
                .flexible(true)
                .quote_style(csv::QuoteStyle::Always)
                .from_writer(&mut output);
            if let Some(header) = &self.header {
                writer
                    .write_record(header.iter())
                    .map_err(error::write_error)?;
            }
            while rows < row_target {
                let Some(record) = self.records.try_next().await? else {
                    break;
                };
                writer
                    .write_record(record.iter())
                    .map_err(error::write_error)?;
                writer.flush().map_err(error::input_error)?;
                rows += 1;
                if writer.get_ref().len() >= byte_target {
                    break;
                }
            }
            writer.flush().map_err(error::input_error)?;
        }
        if rows == 0 && (self.emitted || output.is_empty()) {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(output.into()))
    }
}
