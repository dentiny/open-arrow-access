use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ObjectRef, ReadRequest, ReaderFactory,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, async_trait, bytes_reader,
    read_lines,
};

use super::{AtifAccessor, AtifFormat, error::decode_error, reader::create_reader};
use crate::scan::chunks::RecordChunks;

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
            lines: read_lines(input, None, "frame_atif").boxed(),
            next_index: 1,
            source: request.object.clone(),
        };
        let records = stream::try_unfold(frames, |mut frames| async move {
            Ok(frames.next_record().await?.map(|record| (record, frames)))
        })
        .boxed();
        Ok(RecordChunks::new(records).into_plan(
            metadata,
            request.options.batch_size.get(),
            self.partition_options.target_bytes.get(),
            |(line, _)| line.len(),
            move |records, metadata| {
                let request = request.clone();
                let projection = projection.clone();
                let first_index = records[0].1;
                // Keep original line endings/whitespace for raw_json fidelity.
                let capacity = records.iter().map(|(line, _)| line.len()).sum();
                let mut bytes = Vec::with_capacity(capacity);
                for (line, _) in records {
                    bytes.extend_from_slice(&line);
                }
                Box::pin(async move {
                    let input = Box::new(bytes_reader(bytes.into(), Some(request.object.clone())));
                    Ok(Box::new(create_reader(
                        input,
                        format,
                        request.options.batch_size.get(),
                        projection,
                        metadata,
                        first_index,
                    )) as Box<dyn RecordReader>)
                })
            },
        ))
    }
}

struct AtifFrames {
    lines: BoxStream<'static, Result<Bytes>>,
    next_index: u64,
    source: ObjectRef,
}

impl AtifFrames {
    async fn next_record(&mut self) -> Result<Option<(Bytes, u64)>> {
        while let Some(line) = self.lines.try_next().await? {
            let text = std::str::from_utf8(&line)
                .map_err(|error| decode_error(Some(&self.source), self.next_index, error))?;
            if text.trim().is_empty() {
                continue;
            }
            let index = self.next_index;
            self.next_index += 1;
            return Ok(Some((line, index)));
        }
        Ok(None)
    }
}
