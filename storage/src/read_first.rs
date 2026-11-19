//! Sequential whole-object reads that learn length from the first bounded GET.
use std::num::NonZeroUsize;

use futures::{TryStreamExt, stream};
use open_arrow_access_core::{ByteMetadata, ByteReadRequest, ByteReader, Result, StreamReader};
use opendal::{Buffer, BufferStream, Operator};

use crate::error::{input_too_long, map_error, short_read};

pub(crate) fn open(
    operator: Operator,
    request: ByteReadRequest,
    output_chunk_size: NonZeroUsize,
    default_io_chunk_size: usize,
) -> Box<dyn ByteReader> {
    let metadata = ByteMetadata {
        source: Some(request.object.clone()),
        content_length: None,
        version: None,
    };
    let state = ReadFirst {
        operator,
        path: request.object.locator().to_owned(),
        request_size: request
            .options
            .io_chunk_size
            .map_or(default_io_chunk_size, |v| v.get()) as u64,
        output_chunk_size: output_chunk_size.get(),
        length: None,
        etag: None,
        offset: 0,
        range_size: 0,
        received: 0,
        reader: None,
        stream: None,
        pending: Buffer::new(),
        done: false,
    };
    let batches = stream::try_unfold(state, |mut state| async move {
        let chunk = state.next().await?;
        Ok(chunk.map(|chunk| (chunk, state)))
    });
    Box::new(StreamReader::bytes(batches, metadata))
}

struct ReadFirst {
    operator: Operator,
    path: String,
    request_size: u64,
    output_chunk_size: usize,
    length: Option<u64>,
    etag: Option<String>,
    offset: u64,
    range_size: u64,
    received: u64,
    reader: Option<opendal::Reader>,
    stream: Option<BufferStream>,
    pending: Buffer,
    done: bool,
}

impl ReadFirst {
    async fn metadata(&mut self) -> Result<()> {
        if self.length.is_some() {
            return Ok(());
        }
        let metadata = match self
            .reader
            .as_ref()
            .and_then(|reader| reader.metadata())
            .cloned()
        {
            Some(metadata) => metadata,
            None => self
                .operator
                .stat(&self.path)
                .await
                .map_err(|e| map_error("stat", &self.path, e))?,
        };
        crate::validation::validate_regular_file(metadata.is_file())?;
        self.length = Some(metadata.content_length());
        if self.operator.info().capability().read_with_if_match {
            self.etag = metadata.etag().map(str::to_owned);
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<open_arrow_access_core::Bytes>> {
        loop {
            if !self.pending.is_empty() {
                let size = self.pending.len().min(self.output_chunk_size);
                return Ok(Some(self.pending.split_to(size).to_bytes()));
            }
            if self.done {
                return Ok(None);
            }
            if self.stream.is_none() {
                let mut end = self.offset.saturating_add(self.request_size);
                if let Some(length) = self.length {
                    end = end.min(length);
                }
                if end == self.offset {
                    self.done = true;
                    continue;
                }
                // No `.chunk(...)`: the explicit bounded range avoids OpenDAL's
                // unbounded-range length lookup while still limiting every GET.
                let mut builder = self.operator.reader_with(&self.path);
                if let Some(etag) = self.etag.as_deref() {
                    builder = builder.if_match(etag);
                }
                let reader = builder
                    .await
                    .map_err(|e| map_error("open", &self.path, e))?;
                self.stream = Some(
                    reader
                        .clone()
                        .into_stream(self.offset..end)
                        .await
                        .map_err(|e| map_error("read", &self.path, e))?,
                );
                self.reader = Some(reader);
                self.range_size = end - self.offset;
                self.received = 0;
            }
            let next = self.stream.as_mut().unwrap().try_next().await;
            // Some backends reject any range on an empty object. Confirm only
            // that edge with stat; nonempty range errors remain failures.
            if self.offset == 0
                && self.received == 0
                && let Err(error) = &next
                && error.kind() == opendal::ErrorKind::RangeNotSatisfied
            {
                let metadata = self
                    .operator
                    .stat(&self.path)
                    .await
                    .map_err(|e| map_error("stat", &self.path, e))?;
                crate::validation::validate_regular_file(metadata.is_file())?;
                if metadata.content_length() == 0 {
                    self.stream = None;
                    self.reader = None;
                    self.done = true;
                    continue;
                }
            }
            let next = next.map_err(|e| map_error("read", &self.path, e))?;
            self.metadata().await?;
            let expected = self
                .range_size
                .min(self.length.unwrap().saturating_sub(self.offset));
            match next {
                Some(buffer) => {
                    let size = buffer.len() as u64;
                    if size > expected.saturating_sub(self.received) {
                        return Err(input_too_long());
                    }
                    self.received += size;
                    self.pending = buffer;
                }
                None => {
                    if self.received != expected {
                        return Err(short_read("read"));
                    }
                    self.offset += self.received;
                    self.done = self.offset == self.length.unwrap();
                    self.stream = None;
                    self.reader = None;
                }
            }
        }
    }
}
