use std::io;

use futures::{StreamExt, TryStreamExt, io::AsyncBufRead};
use open_arrow_access_core::{
    ByteReader, Capability, Error, ErrorKind, ObjectRef, ReadState, Reader, Result, async_trait,
    into_stream,
};
use warc::RecordIter;

use super::{WarcRecord, decode_error};

type Input = Box<dyn AsyncBufRead + Unpin + Send>;

pub(super) struct WarcReader {
    records: RecordIter<Input>,
    source: ObjectRef,
    state: ReadState,
}

impl WarcReader {
    pub(super) fn new(input: Box<dyn ByteReader>, source: ObjectRef) -> Self {
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
            records: warc::WarcReader::new(input).iter_records(),
            source,
            state: ReadState::default(),
        }
    }
}

#[async_trait]
impl Reader for WarcReader {
    type Item = WarcRecord;
    type Metadata = ObjectRef;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ObjectRef {
        &self.source
    }

    async fn next(&mut self) -> Result<Option<WarcRecord>> {
        if self.state.is_finished() {
            return Ok(None);
        }
        let result = self.records.next().await.transpose().map_err(decode_error);
        if matches!(result, Ok(None)) {
            self.state.mark_input_eof();
        }
        self.state.observe(&result);
        result
    }
}
