use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ObjectRef, ReadRequest, ReaderFactory,
    RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait,
    read_lines,
};

use super::{AtifAccessor, AtifFormat, error::decode_error, reader::create_reader};
use crate::{DecodePartitionOptions, scan::input::ChunkInput};

#[async_trait]
impl<B> ScanPlanner for AtifAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = *self.binding.factory();
        if format == AtifFormat::Json {
            return Ok(ScanPlan::whole_input(self.open(request).await?));
        }
        let (schema, projection) = format.prepare(&request)?;
        let metadata = RecordMetadata {
            schema,
            source: Some(request.object.clone()),
        };
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let frames = AtifFrames {
            lines: read_lines(
                input,
                Some(self.partition_options.max_record_bytes.get()),
                "frame_atif",
            )
            .boxed(),
            pending: None,
            next_index: 1,
            options: self.partition_options,
            rows: request.options.batch_size.get(),
            source: request.object.clone(),
        };
        let output = metadata.clone();
        let partitions = stream::try_unfold(frames, move |mut frames| {
            let request = request.clone();
            let metadata = output.clone();
            let projection = projection.clone();
            async move {
                let Some((bytes, first_index)) = frames.next_chunk().await? else {
                    return Ok(None);
                };
                let partition: ScanPartition = Box::pin(async move {
                    let input = ChunkInput(bytes)
                        .open(ByteReadRequest::from(&request))
                        .await?;
                    Ok(Box::new(create_reader(
                        input,
                        format,
                        request.options.batch_size.get(),
                        projection,
                        metadata,
                        first_index,
                    )) as Box<dyn RecordReader>)
                });
                Ok(Some((partition, frames)))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

struct AtifFrames {
    lines: BoxStream<'static, Result<Bytes>>,
    pending: Option<Bytes>,
    next_index: u64,
    options: DecodePartitionOptions,
    rows: usize,
    source: ObjectRef,
}

impl AtifFrames {
    async fn next_chunk(&mut self) -> Result<Option<(Bytes, u64)>> {
        let first_index = self.next_index;
        let mut output = Vec::new();
        let mut rows = 0;
        while rows < self.rows {
            let line = match self.pending.take() {
                Some(line) => line,
                None => match self.lines.try_next().await? {
                    Some(line) => line,
                    None => break,
                },
            };
            let text = std::str::from_utf8(&line)
                .map_err(|error| decode_error(Some(&self.source), self.next_index, error))?;
            if text.trim().is_empty() {
                continue;
            }
            if rows > 0 && output.len().saturating_add(line.len()) > self.options.target_bytes.get()
            {
                self.pending = Some(line);
                break;
            }
            // Keep original line endings/whitespace for raw_json fidelity.
            output.extend_from_slice(&line);
            self.next_index += 1;
            rows += 1;
        }
        Ok((rows > 0).then(|| (output.into(), first_index)))
    }
}
