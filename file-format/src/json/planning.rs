use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt,
    RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait,
};

use super::JsonAccessor;
use crate::{
    DecodePartitionOptions,
    scan::{error, input::ChunkInput},
};

#[async_trait]
impl<B> ScanPlanner for JsonAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory().clone();
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
        let frames = JsonFrames {
            input,
            pending_input: Bytes::new(),
            pending_record: None,
            eof: false,
            options: self.partition_options,
            rows: request.options.batch_size.get(),
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

/// Frame complete JSON objects, including pretty-printed objects. Structural
/// scanning stays sequential; each owned chunk is decoded by an independent worker.
struct JsonFrames {
    input: Box<dyn ByteReader>,
    pending_input: Bytes,
    pending_record: Option<Bytes>,
    eof: bool,
    options: DecodePartitionOptions,
    rows: usize,
}

impl JsonFrames {
    async fn next_record(&mut self) -> Result<Option<Bytes>> {
        let mut raw = Vec::new();
        let mut depth = 0;
        let mut quoted = false;
        let mut escaped = false;
        loop {
            if self.pending_input.is_empty() && !self.eof {
                match self.input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(error::invalid_frame("frame_json", "empty byte chunk"));
                    }
                    Some(bytes) => self.pending_input = bytes,
                    None => self.eof = true,
                }
            }
            if self.pending_input.is_empty() && self.eof {
                return if raw.is_empty() {
                    Ok(None)
                } else {
                    Err(error::invalid_frame("frame_json", "truncated JSON object"))
                };
            }
            let mut consumed = 0;
            for &byte in self.pending_input.iter() {
                consumed += 1;
                if raw.is_empty() {
                    if matches!(byte, b' ' | b'\r' | b'\n' | b'\t') {
                        continue;
                    }
                    if byte != b'{' {
                        return Err(error::invalid_frame("frame_json", "expected a JSON object"));
                    }
                }
                if raw.len() == self.options.max_record_bytes.get() {
                    return Err(error::record_too_large("frame_json"));
                }
                raw.push(byte);
                if quoted {
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        quoted = false;
                    }
                } else {
                    match byte {
                        b'"' => quoted = true,
                        b'{' | b'[' => depth += 1,
                        b'}' | b']' => depth -= 1,
                        _ => {}
                    }
                    if depth == 0 {
                        let _ = self.pending_input.split_to(consumed);
                        return Ok(Some(raw.into()));
                    }
                }
            }
            let _ = self.pending_input.split_to(consumed);
        }
    }

    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let mut output = Vec::new();
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
            output.push(b'\n');
            rows += 1;
        }
        Ok((rows > 0).then(|| output.into()))
    }
}
