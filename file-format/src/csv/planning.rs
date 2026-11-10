use csv_core::ReadRecordResult;
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream, Fuse},
};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt,
    RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait,
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
        let target_bytes = self.partition_options.target_bytes.get();
        // Initialization runs on first poll, keeping partition discovery lazy.
        let partitions = stream::once(async move {
            let frames = CsvFrames::new(input, dialect).await?;
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

struct CsvFrames {
    input: Fuse<BoxStream<'static, Result<Bytes>>>,
    pending_input: Bytes,
    parser: csv_core::Reader,
    prefix: Vec<u8>,
    // Emit a header-only input once so Arrow can validate it even without rows.
    emitted: bool,
}

impl CsvFrames {
    async fn new(input: Box<dyn ByteReader>, dialect: CsvDialect) -> Result<Self> {
        let mut input = stream::try_unfold(input, |mut input| async move {
            match input.next().await? {
                Some(bytes) if bytes.is_empty() => {
                    Err(error::invalid_frame("frame_csv", "empty byte chunk"))
                }
                bytes => Ok(bytes.map(|bytes| (bytes, input))),
            }
        })
        .boxed()
        .fuse();
        // Gather only a possible source-start BOM; stop as soon as it is ruled out.
        // Retain the rest of the first chunk in the ordinary input buffer.
        let mut pending_input = Bytes::new();
        while pending_input.len() < 3 && b"\xef\xbb\xbf".starts_with(&pending_input) {
            let Some(bytes) = input.try_next().await? else {
                break;
            };
            pending_input = if pending_input.is_empty() {
                bytes
            } else {
                [pending_input.as_ref(), bytes.as_ref()].concat().into()
            };
        }
        if pending_input.starts_with(b"\xef\xbb\xbf") {
            let _ = pending_input.split_to(3);
        }
        let mut frames = Self {
            input,
            pending_input,
            parser: dialect.parser(),
            // A skipped blank line prevents fresh Arrow parsers from stripping
            // a BOM that belongs to a later data record. Replay real headers only.
            prefix: vec![dialect.terminator.unwrap_or(b'\n')],
            emitted: false,
        };
        if dialect.has_header {
            let header = frames.next_record().await?.unwrap_or_default();
            frames.prefix.extend_from_slice(&header);
        }
        Ok(frames)
    }

    async fn next_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut raw = Vec::new();
        loop {
            if self.pending_input.is_empty() {
                self.pending_input = self.input.try_next().await?.unwrap_or_default();
            }
            // Discard decoded fields: csv-core is used only to identify safe raw
            // record boundaries with the same quoting/escape rules as Arrow CSV.
            let mut output = [0; 8192];
            let mut ends = [0; 128];
            let (result, consumed, _, _) =
                self.parser
                    .read_record(&self.pending_input, &mut output, &mut ends);
            raw.extend_from_slice(&self.pending_input.split_to(consumed));
            match result {
                ReadRecordResult::Record => return Ok(Some(raw)),
                ReadRecordResult::End => return Ok(None),
                _ => {}
            }
        }
    }

    async fn next_chunk(&mut self, row_target: usize, byte_target: usize) -> Result<Option<Bytes>> {
        let mut output = self.prefix.clone();
        let mut rows = 0;
        // The byte target is soft: stop after a complete record reaches it.
        while rows < row_target && (rows == 0 || output.len() < byte_target) {
            let Some(record) = self.next_record().await? else {
                break;
            };
            output.extend_from_slice(&record);
            rows += 1;
        }
        if rows == 0 && self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(output.into()))
    }
}
