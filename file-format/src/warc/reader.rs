use std::io;

use futures::{StreamExt, TryStreamExt, io::AsyncBufRead};
use open_arrow_access_core::{
    ByteReader, Capability, Error, ErrorKind, Reader, RecordBatch, RecordMetadata, Result,
    async_trait, into_stream,
};
use warc::{EmptyBody, RawRecordIter, Record};

use super::{batch::WarcBatch, decode_error};

type Input = Box<dyn AsyncBufRead + Unpin + Send>;

pub(super) struct WarcReader {
    records: RawRecordIter<Input>,
    metadata: RecordMetadata,
    indices: Vec<usize>,
    batch_size: usize,
    batch_capacity: usize,
    batch_body_bytes: usize,
    finished: bool,
}

impl WarcReader {
    pub(super) fn new(
        input: Box<dyn ByteReader>,
        metadata: RecordMetadata,
        indices: Vec<usize>,
        batch_size: usize,
        batch_body_bytes: usize,
    ) -> Self {
        let stream = into_stream(input).map(|result| {
            result
                .and_then(|bytes| {
                    if bytes.is_empty() {
                        Err(Error::new(
                            ErrorKind::Io,
                            "read_warc",
                            "byte reader emitted an empty chunk",
                        ))
                    } else {
                        Ok(bytes)
                    }
                })
                .map_err(io::Error::other)
        });
        let input: Input = Box::new(stream.into_async_read());
        Self {
            records: warc::WarcReader::new(input).iter_raw_records(),
            metadata,
            indices,
            batch_size,
            batch_capacity: batch_size,
            batch_body_bytes,
            finished: false,
        }
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut batch = WarcBatch::new(
            self.metadata.schema.clone(),
            &self.indices,
            self.batch_capacity,
        );
        while batch.len < self.batch_size && batch.body_bytes < self.batch_body_bytes {
            match self.records.next().await {
                Some(Ok((headers, body))) => {
                    let record = Record::<EmptyBody>::try_from(headers).map_err(decode_error)?;
                    batch.append(record, body)?;
                }
                Some(Err(error)) => return Err(decode_error(error)),
                None => {
                    self.finished = true;
                    break;
                }
            }
        }
        if batch.len == 0 {
            return Ok(None);
        }
        self.batch_capacity = batch.len;
        batch.finish().map(Some)
    }
}

#[async_trait]
impl Reader for WarcReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            list: false,
        }
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        let result = self.read_batch().await;
        if !matches!(result, Ok(Some(_))) {
            self.finished = true;
        }
        result
    }
}
