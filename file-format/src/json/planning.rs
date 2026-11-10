use bytes::BytesMut;
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt,
    RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, bytes_reader,
};
use serde_json::{Deserializer, value::RawValue};

use super::{JsonAccessor, error};
use crate::scan::chunks::RecordChunks;

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
        };
        let records = stream::try_unfold(frames, |mut frames| async move {
            Ok(frames.next_record().await?.map(|record| (record, frames)))
        })
        .boxed();
        let chunks = RecordChunks::new(records);
        let rows = request.options.batch_size.get();
        let target_bytes = self.partition_options.target_bytes.get();
        let partitions = stream::try_unfold(chunks, move |mut frames| {
            let format = format.clone();
            let request = request.clone();
            async move {
                let Some(records) = frames.next_chunk(rows, target_bytes, Bytes::len).await? else {
                    return Ok(None);
                };
                let mut bytes = Vec::new();
                for record in records {
                    bytes.extend_from_slice(&record);
                    bytes.push(b'\n');
                }
                let partition: ScanPartition = Box::pin(async move {
                    let input = bytes_reader(bytes.into(), Some(request.object.clone()));
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
}
