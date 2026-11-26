//! Readers keep range access separate from sequential response consumption.

use std::{num::NonZeroUsize, ops::Range};

use futures::TryStreamExt;
use open_arrow_access_core::{
    ByteMetadata, BytePosition, ByteReader, Bytes, Capability, ObjectRef, Reader, Result,
    async_trait, validate_byte_range,
};

use crate::error::{map_error, short_read};

const MAX_FETCH_BATCH_SIZE: u64 = 2 * 1024 * 1024;
// Bound scattered staging as well as payload bytes: tiny network responses
// can each retain a receive allocation much larger than their payload.
const MAX_FETCH_RANGES: usize = 512;

/// A fetched byte interval whose bounds match its payload.
/// Keep the fields private so cache slicing and prefix reuse share this invariant.
pub(super) struct PrefetchedRange {
    range: Range<u64>,
    bytes: Bytes,
}

impl PrefetchedRange {
    pub(super) fn new(range: Range<u64>, bytes: Bytes, content_length: u64) -> Result<Self> {
        validate_byte_range(&range, content_length, "prefetch")?;
        crate::validation::validate_read_length(
            bytes.len() as u64,
            range.end - range.start,
            "prefetch",
        )?;
        Ok(Self { range, bytes })
    }

    pub(super) fn sequential_prefix(&self) -> Option<opendal::Buffer> {
        (self.range.start == 0).then(|| opendal::Buffer::from(self.bytes.clone()))
    }

    fn slice(&self, range: &Range<u64>) -> Option<Bytes> {
        if range.start >= self.range.start && range.end <= self.range.end {
            return Some(self.bytes.slice(
                (range.start - self.range.start) as usize..(range.end - self.range.start) as usize,
            ));
        }
        None
    }
}

/// A reader with independently positioned exact-range operations.
/// Its sequential cursor uses a bounded stream; range reads never touch it.
pub(super) struct RangedReader {
    inner: opendal::Reader,
    sequential: SequentialReader,
    prefetched: Option<PrefetchedRange>,
}

impl RangedReader {
    pub(super) fn new(inner: opendal::Reader, sequential: SequentialReader) -> Self {
        Self {
            inner,
            sequential,
            prefetched: None,
        }
    }

    pub(super) fn with_prefetched(mut self, prefetched: PrefetchedRange) -> Self {
        self.prefetched = Some(prefetched);
        self
    }

    fn cached_range(&self, range: &Range<u64>) -> Option<Bytes> {
        self.prefetched.as_ref()?.slice(range)
    }

    async fn read_exact_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        validate_byte_range(&range, self.sequential.length(), "read_range")?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        if let Some(bytes) = self.cached_range(&range) {
            return Ok(bytes);
        }
        let path = self.sequential.path();
        let expected = range.end - range.start;
        if expected > MAX_FETCH_BATCH_SIZE {
            // Parquet requires contiguous Bytes. Copy incrementally into the
            // final allocation rather than retaining every fragment as well.
            let capacity = crate::validation::range_capacity(expected)?;
            let mut output = Vec::with_capacity(capacity);
            let read = self
                .inner
                .read_into(&mut output, range)
                .await
                .map_err(|error| map_error("read", path, error))?;
            crate::validation::validate_read_length(read as u64, capacity as u64, "read_range")?;
            return Ok(Bytes::from(output));
        }
        let buffer = self
            .inner
            .read(range)
            .await
            .map_err(|error| map_error("read", path, error))?;
        crate::validation::validate_read_length(buffer.len() as u64, expected, "read_range")?;
        Ok(buffer.to_bytes())
    }

    async fn fetch_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let lengths = ranges
            .iter()
            .map(|range| range.end - range.start)
            .collect::<Vec<_>>();
        let path = self.sequential.path();
        let buffers = self
            .inner
            .fetch(ranges)
            .await
            .map_err(|error| map_error("read_ranges", path, error))?;
        buffers
            .into_iter()
            .zip(lengths)
            .map(|(buffer, expected)| {
                crate::validation::validate_read_length(
                    buffer.len() as u64,
                    expected,
                    "read_ranges",
                )?;
                Ok(compact_range_buffer(buffer))
            })
            .collect()
    }
}

#[async_trait]
impl Reader for RangedReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        self.sequential.metadata()
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.sequential.next().await
    }
}

#[async_trait]
impl ByteReader for RangedReader {
    fn position(&self) -> Option<BytePosition> {
        self.sequential.position()
    }

    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.read_exact_range(range).await
    }

    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        for range in &ranges {
            validate_byte_range(range, self.sequential.length(), "read_range")?;
        }
        if ranges.iter().all(Range::is_empty) {
            return Ok(vec![Bytes::new(); ranges.len()]);
        }
        if let Some(cached) = ranges
            .iter()
            .map(|range| {
                if range.is_empty() {
                    Some(Bytes::new())
                } else {
                    self.cached_range(range)
                }
            })
            .collect::<Option<Vec<_>>>()
        {
            return Ok(cached);
        }
        let mut output = Vec::with_capacity(ranges.len());
        let mut start = 0;
        // Keep fragmented staging bounded. Large ranges are assembled directly;
        // small ranges retain fetch's coalescing/concurrency within each batch.
        while start < ranges.len() {
            if ranges[start].end - ranges[start].start > MAX_FETCH_BATCH_SIZE {
                output.push(self.read_exact_range(ranges[start].clone()).await?);
                start += 1;
                continue;
            }
            let mut end = start;
            let mut size = 0;
            while end < ranges.len() {
                if end - start >= MAX_FETCH_RANGES {
                    let previous = &ranges[end - 1];
                    let next = &ranges[end];
                    // Keep adjacent/overlapping runs together so limiting
                    // scattered work does not split a coalesced backend read.
                    if next.start > previous.end || next.end < previous.start {
                        break;
                    }
                }
                let length = ranges[end].end - ranges[end].start;
                if length > MAX_FETCH_BATCH_SIZE - size {
                    break;
                }
                size += length;
                end += 1;
            }
            output.extend(self.fetch_ranges(ranges[start..end].to_vec()).await?);
            start = end;
        }
        Ok(output)
    }
}

/// Owns one sequential response and its delivered-byte cursor. The stream is
/// absent only after EOF/error, releasing both the response and buffered bytes.
pub(super) struct SequentialReader {
    input: Option<BufferedStream>,
    metadata: ByteMetadata,
    offset: u64,
    chunk_size: NonZeroUsize,
}

impl SequentialReader {
    pub(super) fn new(
        stream: opendal::BufferStream,
        object: ObjectRef,
        len: u64,
        chunk_size: NonZeroUsize,
    ) -> Self {
        Self {
            input: Some(BufferedStream {
                stream,
                pending: opendal::Buffer::new(),
            }),
            metadata: ByteMetadata {
                source: Some(object),
                content_length: Some(len),
                version: None,
            },
            offset: 0,
            chunk_size,
        }
    }

    pub(super) fn with_prefix(
        stream: opendal::BufferStream,
        object: ObjectRef,
        len: u64,
        chunk_size: NonZeroUsize,
        prefix: Option<opendal::Buffer>,
    ) -> Self {
        let mut reader = Self::new(stream, object, len, chunk_size);
        if let Some(prefix) = prefix {
            let input = reader.input.as_mut().expect("new reader owns its input");
            input.pending = prefix;
        }
        reader
    }

    fn length(&self) -> u64 {
        // Construction always supplies a length; metadata is immutable after it.
        self.metadata
            .content_length
            .expect("OpenDAL readers have a known content length")
    }

    fn path(&self) -> &str {
        self.metadata
            .source
            .as_ref()
            .expect("OpenDAL readers have a source")
            .locator()
    }
}

#[async_trait]
impl Reader for SequentialReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        let len = self.length();
        let path = self
            .metadata
            .source
            .as_ref()
            .expect("OpenDAL readers have a source")
            .locator();
        let Some(input) = &mut self.input else {
            return Ok(None);
        };
        let result = if self.offset == len {
            // Even after delivering the declared bytes, consume actual body EOF
            // so a successful response is not cancelled instead of reused.
            input.finish(path).await.map(|()| None)
        } else {
            let target = (len - self.offset).min(self.chunk_size.get() as u64) as usize;
            input
                .read_chunk(path, target, len, &mut self.offset)
                .await
                .map(Some)
        };
        if !matches!(&result, Ok(Some(_))) {
            self.input = None;
        }
        result
    }
}

#[async_trait]
impl ByteReader for SequentialReader {
    fn position(&self) -> Option<BytePosition> {
        Some(BytePosition::Stored(self.offset))
    }
    // Nonempty range requests fail without acquiring a second permit while
    // the response is unread. An empty bulk request is always a no-op.
    async fn read_range(&mut self, _range: Range<u64>) -> Result<Bytes> {
        Err(crate::error::sequential_range_unsupported())
    }

    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        Err(crate::error::sequential_range_unsupported())
    }
}

/// Transport buffers can exceed emitted chunks. Keep unread bytes together with
/// the response that owns them; dropping this value releases both immediately.
struct BufferedStream {
    stream: opendal::BufferStream,
    pending: opendal::Buffer,
}

impl BufferedStream {
    async fn finish(&mut self, path: &str) -> Result<()> {
        while let Some(buffer) = self
            .stream
            .try_next()
            .await
            .map_err(|error| map_error("read", path, error))?
        {
            crate::validation::validate_chunk_length(buffer.len() as u64, 0)?;
        }
        Ok(())
    }

    async fn read_chunk(
        &mut self,
        path: &str,
        target: usize,
        len: u64,
        offset: &mut u64,
    ) -> Result<Bytes> {
        let mut output = Vec::new();
        let mut written = 0;
        while written < target {
            while self.pending.is_empty() {
                let Some(buffer) = self
                    .stream
                    .try_next()
                    .await
                    .map_err(|error| map_error("read", path, error))?
                else {
                    return Err(short_read("read"));
                };
                crate::validation::validate_chunk_length(buffer.len() as u64, len - *offset)?;
                self.pending = buffer;
            }
            if written == 0 && self.pending.current().len() >= target {
                *offset += target as u64;
                return Ok(self.pending.split_to(target).next().unwrap());
            }
            let count = (target - written).min(self.pending.len());
            output.extend(self.pending.split_to(count));
            *offset += count as u64;
            written += count;
        }
        Ok(opendal::Buffer::from(output).to_bytes())
    }
}

// Copy small responses so returned slices do not retain large network allocations.
fn compact_range_buffer(buffer: opendal::Buffer) -> Bytes {
    if buffer.len() > 4096 {
        return buffer.to_bytes();
    }
    Bytes::from(buffer.to_vec())
}
