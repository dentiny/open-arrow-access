use bytes::BytesMut;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt,
    RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, bytes_reader,
};
use serde_json::{Deserializer, value::RawValue};

use super::{JsonAccessor, error};
use crate::DecodePartitionOptions;

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
            pending_input: BytesMut::new(),
            pending_record: None,
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
                let partition: ScanPartition = Box::pin(async move {
                    let input = bytes_reader(bytes, Some(request.object.clone()));
                    format.open_input(Box::new(input), request).await
                });
                Ok(Some((partition, frames)))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

/// Frame complete JSON objects with serde_json, retaining the original bytes.
/// Framing stays sequential; each owned chunk is decoded by an independent worker.
struct JsonFrames {
    input: Box<dyn ByteReader>,
    pending_input: BytesMut,
    pending_record: Option<Bytes>,
    options: DecodePartitionOptions,
    rows: usize,
}

impl JsonFrames {
    async fn next_record(&mut self) -> Result<Option<Bytes>> {
        loop {
            let mut records =
                Deserializer::from_slice(&self.pending_input).into_iter::<&RawValue>();
            match records.next() {
                Some(Ok(record)) => {
                    if !record.get().starts_with('{') {
                        return Err(error::invalid_frame("frame_json", "expected a JSON object"));
                    }
                    let consumed = records.byte_offset();
                    return Ok(Some(self.pending_input.split_to(consumed).freeze()));
                }
                Some(Err(source)) if !source.is_eof() => {
                    return Err(error::json_frame(source));
                }
                _ => {}
            }
            // serde_json retries incomplete values from their start after more input arrives.
            let consumed = records.byte_offset();
            let _ = self.pending_input.split_to(consumed);
            match self.input.next().await? {
                Some(bytes) if bytes.is_empty() => {
                    return Err(error::invalid_frame("frame_json", "empty byte chunk"));
                }
                Some(bytes) => self.pending_input.extend_from_slice(&bytes),
                None if self.pending_input.is_empty() => return Ok(None),
                None => {
                    return Err(error::invalid_frame("frame_json", "truncated JSON object"));
                }
            }
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
