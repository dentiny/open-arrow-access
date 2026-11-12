use std::ops::Range;

use futures::stream::BoxStream;

use crate::{
    Bytes, Capability, ObjectRef, RecordBatch, RecordCapability, Result, SchemaRef, async_trait,
};

/// A pull-based asynchronous reader. Metadata is available before the first item.
///
/// `next()` returning `Ok(None)` is EOF; its EOF and errors are terminal. Dropping a reader stops this
/// library's work: implementations must not leave detached producer tasks running.
/// A future returned by `next` is Send. Cancelling a pending call need not make a
/// stateful decoder resumable; drop the reader after cancelling such a call.
/// Implementations use the re-exported `#[async_trait]` attribute for their
/// asynchronous methods while retaining support for `dyn` readers.
/// [`crate::ReadState`] provides shared input-EOF and terminal-result tracking.
#[async_trait]
pub trait Reader: Send {
    /// Unit emitted by a successful read, such as Bytes or RecordBatch.
    type Item: Send + 'static;
    /// Typed description of the reader's output representation.
    type Metadata: Send + Sync + 'static;

    /// Capability type of this representation: byte operations or record operations
    /// with a mandatory ordering contract.
    type Capability: Copy + Send + Sync + 'static;

    /// Report operations on this opened representation without I/O.
    ///
    /// Wrappers must report their effective abilities rather than blindly retain
    /// upstream flags. Reader capabilities never advertise accessor-only listing.
    fn capability(&self) -> Self::Capability;

    /// Borrow output metadata without starting another I/O operation.
    ///
    /// Metadata is available as soon as [`crate::Accessor::open`] succeeds. A
    /// record reader's schema describes every batch it emits; a byte reader's
    /// lengths and coordinates describe its current, possibly decoded, bytes.
    fn metadata(&self) -> &Self::Metadata;
    /// Pull the next item, returning `Ok(None)` at end of input.
    ///
    /// Consumers control demand by polling this future. Byte readers emit nonempty
    /// chunks, and record readers preserve their logical input order by default.
    /// Stop consuming after EOF or an error. After cancelling an in-flight call,
    /// drop a stateful reader rather than assuming its decoder can resume safely.
    ///
    /// # Errors
    ///
    /// Returns failures encountered while fetching or decoding the next item.
    /// An error terminates consumption; implementations must not transparently
    /// replay items that have already been delivered.
    async fn next(&mut self) -> Result<Option<Self::Item>>;
}

/// Metadata about the bytes currently exposed by a reader.
#[derive(Clone, Debug, Default)]
pub struct ByteMetadata {
    pub source: Option<ObjectRef>,
    /// Length in this reader's byte coordinates. A decoder must not retain the
    /// compressed length here when its decoded length is unknown.
    pub content_length: Option<u64>,
    /// Optional upstream object version; absence means no snapshot guarantee.
    pub version: Option<String>,
}

/// Byte specialization used by storage, byte layers, and decompression wrappers.
/// Sequential chunks must be nonempty; use None for EOF.
#[async_trait]
pub trait ByteReader:
    Reader<Item = Bytes, Metadata = ByteMetadata, Capability = Capability>
{
    /// Read exactly the half-open `range` in the current byte representation.
    ///
    /// Range reads leave the sequential cursor unchanged. Valid empty ranges
    /// return empty Bytes. A wrapper must delegate or explicitly remove range
    /// support; it must not advertise it by repeatedly decoding from byte zero.
    /// [`Capability::read_with_range`] must match the implementation's support.
    ///
    /// # Errors
    ///
    /// The default returns [`ErrorKind::Unsupported`]. Implementations reject
    /// reversed or out-of-bounds ranges and report short reads or I/O failures
    /// instead of silently returning fewer bytes than requested.
    async fn read_range(&mut self, _range: Range<u64>) -> Result<Bytes> {
        Err(crate::error::default_range_unsupported())
    }

    /// Read exact ranges in request order without moving the sequential cursor.
    /// Empty ranges yield empty Bytes; an empty request yields an empty vector.
    /// The default calls `read_range` sequentially. Backends may coalesce or
    /// concurrently fetch ranges within the configured I/O limit. Wrappers must
    /// delegate this operation or retain the fallback to intercept every range.
    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let mut output = Vec::with_capacity(ranges.len());
        for range in ranges {
            output.push(self.read_range(range).await?);
        }
        Ok(output)
    }
}

#[derive(Clone, Debug)]
pub struct RecordMetadata {
    pub schema: SchemaRef,
    pub source: Option<ObjectRef>,
}

/// Record specialization used by formats, datasets, services and record layers.
pub trait RecordReader:
    Reader<Item = RecordBatch, Metadata = RecordMetadata, Capability = RecordCapability>
{
}

impl<T> RecordReader for T where
    T: Reader<Item = RecordBatch, Metadata = RecordMetadata, Capability = RecordCapability>
        + ?Sized
{
}

pub type RecordStream = BoxStream<'static, Result<RecordBatch>>;

/// Convert either specialization to a standard Arrow/bytes-compatible stream.
/// The stream owns the reader and terminates after its first error.
pub fn into_stream<R>(reader: Box<R>) -> BoxStream<'static, Result<R::Item>>
where
    R: Reader + ?Sized + 'static,
{
    Box::pin(futures::stream::try_unfold(
        reader,
        |mut reader| async move { Ok(reader.next().await?.map(|item| (item, reader))) },
    ))
}
