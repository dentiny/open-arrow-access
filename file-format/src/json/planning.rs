use crate::inference::SchemaInference;

use bytes::BytesMut;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ReadRequest, ReaderFactoryExt, RecordMetadata,
    Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, bytes_reader,
};
use serde_json::{Deserializer, value::RawValue};

use super::{JsonAccessor, error};

#[async_trait]
impl<B> ScanPlanner for JsonAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory().clone();
        let prepared = format.prepare_scan(&request)?;
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let (format, input) = format.resolve_schema(input).await?;
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => format
                .prepare_scan(&request)?
                .expect("resolved JSON schema"),
        };
        let metadata = RecordMetadata {
            schema: prepared.schema,
            source: Some(request.object.clone()),
        };
        let frames = JsonFrames {
            input: super::nonfinite::normalize_input(input),
            pending_input: BytesMut::new(),
        };
        let rows = request.options.batch_size.get();
        let target_bytes = self.partition_options.target_bytes.get();
        let partitions = stream::try_unfold(frames, move |mut frames| {
            let request = request.clone();
            let format = format.clone();
            async move {
                let Some(bytes) = frames.next_chunk(rows, target_bytes).await? else {
                    return Ok(None);
                };
                let partition: ScanPartition = Box::pin(async move {
                    let input = bytes_reader(bytes, Some(request.object.clone()));
                    format.open_input(Box::new(input), request).await
                });
                Ok(Some((partition, frames)))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions).with_ordering(self.capability().ordering))
    }
}

/// Frame complete JSON objects with serde_json, retaining the original bytes.
/// Framing stays sequential; each owned chunk is decoded by an independent worker.
struct JsonFrames {
    input: Box<dyn ByteReader>,
    pending_input: BytesMut,
}

impl JsonFrames {
    async fn next_chunk(&mut self, row_target: usize, byte_target: usize) -> Result<Option<Bytes>> {
        let mut framed = 0usize;
        let mut rows = 0usize;
        loop {
            let mut records =
                Deserializer::from_slice(&self.pending_input[framed..]).into_iter::<&RawValue>();
            match records.next() {
                Some(Ok(record)) => {
                    if !record.get().starts_with('{') {
                        return Err(error::invalid_frame("frame_json", "expected a JSON object"));
                    }
                    let end = framed + records.byte_offset();
                    if rows > 0 && end > byte_target {
                        return Ok(Some(self.pending_input.split_to(framed).freeze()));
                    }
                    framed = end;
                    rows += 1;
                    if rows == row_target || framed >= byte_target {
                        return Ok(Some(self.pending_input.split_to(framed).freeze()));
                    }
                    continue;
                }
                Some(Err(source)) if !source.is_eof() => {
                    return Err(error::json_frame(source));
                }
                _ => {}
            }
            // Preserve complete objects while retrying only the incomplete tail.
            // Split once per partition, avoiding per-record buffers and concatenation.
            framed += records.byte_offset();
            if rows > 0 && framed >= byte_target {
                return Ok(Some(self.pending_input.split_to(framed).freeze()));
            }
            if rows == 0 && framed > 0 {
                let _ = self.pending_input.split_to(framed);
                framed = 0;
            }
            match self.input.next().await? {
                Some(bytes) if bytes.is_empty() => {
                    return Err(error::invalid_frame("frame_json", "empty byte chunk"));
                }
                Some(bytes) => self.pending_input.extend_from_slice(&bytes),
                None if framed == self.pending_input.len() => {
                    return Ok((rows > 0).then(|| self.pending_input.split_to(framed).freeze()));
                }
                None => {
                    return Err(error::invalid_frame("frame_json", "truncated JSON object"));
                }
            }
        }
    }
}
