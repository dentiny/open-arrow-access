use bytes::BytesMut;
use futures::stream;
use jiter::{Jiter, JiterErrorType, JsonErrorType, Peek};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, Error, ErrorKind, ReadRequest, ReaderFactory,
    ReaderFactoryExt, RecordMetadata, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait,
    bytes_reader,
};

use super::{JsonAccessor, error};

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

/// Frame complete JSON objects, including nonfinite values, retaining their bytes.
/// Each owned chunk contains complete objects and can be decoded independently.
pub(super) struct JsonFrames {
    pub(super) input: Box<dyn ByteReader>,
    pub(super) pending_input: BytesMut,
}

impl JsonFrames {
    pub(super) async fn next_chunk(
        &mut self,
        row_target: usize,
        byte_target: usize,
    ) -> Result<Option<Bytes>> {
        let mut framed = 0usize;
        let mut rows = 0usize;
        loop {
            let mut records = Jiter::new(&self.pending_input[framed..]).with_allow_inf_nan();
            let object = records.peek().ok() == Some(Peek::Object);
            match records.next_skip() {
                Ok(()) => {
                    if !object {
                        return Err(error::invalid_frame("frame_json", "expected a JSON object"));
                    }
                    let end = framed + records.current_index();
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
                Err(source)
                    if !matches!(
                        source.error_type,
                        JiterErrorType::JsonError(
                            JsonErrorType::EofWhileParsingList
                                | JsonErrorType::EofWhileParsingObject
                                | JsonErrorType::EofWhileParsingString
                                | JsonErrorType::EofWhileParsingValue
                        )
                    ) =>
                {
                    return Err(error::json_frame(source));
                }
                _ => {}
            }
            // Preserve complete objects while retrying only the incomplete tail.
            // Split once per partition, avoiding per-record buffers and concatenation.
            // Skip only whitespace, never bytes from an incomplete record.
            framed += self.pending_input[framed..]
                .iter()
                .take_while(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
                .count();
            if rows > 0 && framed >= byte_target {
                return Ok(Some(self.pending_input.split_to(framed).freeze()));
            }
            if rows == 0 && framed > 0 {
                let _ = self.pending_input.split_to(framed);
                framed = 0;
            }
            match self.input.next().await? {
                Some(bytes) if bytes.is_empty() => {
                    return Err(Error::new(
                        ErrorKind::Io,
                        "read_json",
                        "byte reader emitted an empty chunk",
                    ));
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
