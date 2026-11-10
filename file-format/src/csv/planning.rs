use csv_core::ReadRecordResult;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt,
    RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait,
};

use super::{CsvAccessor, CsvDialect, error};
use crate::{DecodePartitionOptions, scan::input::ChunkInput};

#[async_trait]
impl<B> ScanPlanner for CsvAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let mut format = self.binding.factory().clone();
        let Some(dialect) = format.parallel_dialect else {
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
        // A header prefix prevents each fresh parser from stripping an in-data BOM.
        // Reuse the actual header when present; otherwise skip a synthetic header.
        let synthetic = synthetic_header(dialect, self.binding.factory().schema_len());
        if !dialect.has_header {
            format.format = format
                .format
                .with_header(true)
                .with_header_validation(false);
        }
        let frames = CsvFrames {
            input,
            pending_input: Bytes::new(),
            next_input: None,
            prefix_checked: false,
            eof: false,
            parser: dialect.parser(),
            options: self.partition_options,
            rows: request.options.batch_size.get(),
            header: (!dialect.has_header).then_some(synthetic),
            header_read: !dialect.has_header,
            emitted: false,
            pending_record: None,
        };
        let partitions = stream::try_unfold(frames, move |mut frames| {
            let format = format.clone();
            let request = request.clone();
            async move {
                let Some(bytes) = frames.next_chunk().await? else {
                    return Ok(None);
                };
                let partition: ScanPartition =
                    Box::pin(async move { format.open_reader(&ChunkInput(bytes), request).await });
                Ok(Some((partition, frames)))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

fn synthetic_header(dialect: CsvDialect, columns: usize) -> Vec<u8> {
    let byte = (b'a'..=b'z')
        .find(|byte| {
            *byte != dialect.delimiter
                && *byte != dialect.quote
                && Some(*byte) != dialect.escape
                && Some(*byte) != dialect.comment
                && Some(*byte) != dialect.terminator
        })
        .unwrap();
    let mut header = Vec::new();
    for index in 0..columns {
        if index > 0 {
            header.push(dialect.delimiter);
        }
        header.push(byte);
    }
    header.push(dialect.terminator.unwrap_or(b'\n'));
    header
}

struct CsvFrames {
    input: Box<dyn ByteReader>,
    pending_input: Bytes,
    next_input: Option<Bytes>,
    prefix_checked: bool,
    eof: bool,
    parser: csv_core::Reader,
    options: DecodePartitionOptions,
    rows: usize,
    header: Option<Vec<u8>>,
    header_read: bool,
    emitted: bool,
    pending_record: Option<Vec<u8>>,
}

impl CsvFrames {
    async fn read_input(&mut self) -> Result<Option<Bytes>> {
        match self.next_input.take() {
            Some(bytes) => Ok(Some(bytes)),
            None => self.input.next().await,
        }
    }

    async fn read_prefix(&mut self) -> Result<()> {
        let mut prefix = Vec::new();
        while prefix.len() < 3 {
            match self.input.next().await? {
                Some(bytes) if bytes.is_empty() => {
                    return Err(error::invalid_frame("frame_csv", "empty byte chunk"));
                }
                Some(mut bytes) => {
                    prefix.extend_from_slice(&bytes.split_to((3 - prefix.len()).min(bytes.len())));
                    if !bytes.is_empty() {
                        self.next_input = Some(bytes);
                    }
                    if !b"\xef\xbb\xbf".starts_with(&prefix) {
                        break;
                    }
                }
                None => {
                    self.eof = true;
                    break;
                }
            }
        }
        // Strip only a source-start BOM, before any synthetic header is added.
        // Buffering three bytes also handles a BOM fragmented by storage chunks.
        if prefix != b"\xef\xbb\xbf" {
            self.pending_input = prefix.into();
        }
        self.prefix_checked = true;
        Ok(())
    }

    async fn next_record(&mut self) -> Result<Option<Vec<u8>>> {
        if !self.prefix_checked {
            self.read_prefix().await?;
        }
        let mut raw = Vec::new();
        loop {
            if self.pending_input.is_empty() && !self.eof {
                match self.read_input().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(error::invalid_frame("frame_csv", "empty byte chunk"));
                    }
                    Some(bytes) => self.pending_input = bytes,
                    None => self.eof = true,
                }
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

    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if !self.header_read {
            self.header = self.next_record().await?;
            self.header_read = true;
        }
        let mut output = self.header.clone().unwrap_or_default();
        let mut rows = 0;
        while rows < self.rows {
            let record = match self.pending_record.take() {
                Some(record) => record,
                None => match self.next_record().await? {
                    Some(record) => record,
                    None => break,
                },
            };
            if rows > 0
                && output.len().saturating_add(record.len()) > self.options.target_bytes.get()
            {
                self.pending_record = Some(record);
                break;
            }
            output.extend_from_slice(&record);
            rows += 1;
        }
        if rows == 0 && (self.emitted || output.is_empty()) {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(output.into()))
    }
}
