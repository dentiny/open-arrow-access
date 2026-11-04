use std::ops::Range;

use futures::{future::BoxFuture, stream::BoxStream};

use crate::{Bytes, Error, ErrorKind, ObjectRef, RecordBatch, Result, SchemaRef};

/// A pull-based asynchronous reader. Metadata is available before the first item.
///
/// `Ok(None)` is EOF; EOF and errors are terminal. Dropping a reader stops this
/// library's work: implementations must not leave detached producer tasks running.
/// A future returned by `next` is Send. Cancelling a pending call need not make a
/// stateful decoder resumable; drop the reader after cancelling such a call.
#[deny(missing_docs)]
pub trait Reader: Send {
    /// Unit emitted by a successful read, such as Bytes or RecordBatch.
    type Item: Send + 'static;
    /// Typed description of the reader's output representation.
    type Metadata: Send + Sync + 'static;

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
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Self::Item>>>;
}

/// Abilities of this exact reader, after all input wrappers have been applied.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReaderCapabilities {
    pub range_read: bool,
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
#[deny(missing_docs)]
pub trait ByteReader: Reader<Item = Bytes, Metadata = ByteMetadata> {
    /// Report the abilities of this reader after all wrapping or decoding.
    ///
    /// The default advertises no range access. Wrappers must report what they
    /// actually expose instead of blindly inheriting upstream capabilities.
    fn capabilities(&self) -> ReaderCapabilities {
        ReaderCapabilities::default()
    }

    /// Read exactly the half-open `range` in the current byte representation.
    ///
    /// Range reads leave the sequential cursor unchanged. Valid empty ranges
    /// return empty Bytes. A wrapper must delegate or explicitly remove range
    /// support; it must not advertise it by repeatedly decoding from byte zero.
    ///
    /// # Errors
    ///
    /// The default returns [`ErrorKind::Unsupported`]. Implementations reject
    /// reversed or out-of-bounds ranges and report short reads or I/O failures
    /// instead of silently returning fewer bytes than requested.
    fn read_range(&mut self, _range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async {
            Err(Error::new(
                ErrorKind::Unsupported,
                "read_range",
                "reader is sequential only",
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct RecordMetadata {
    pub schema: SchemaRef,
    pub source: Option<ObjectRef>,
}

/// Record specialization used by formats and future datasets, services and layers.
#[deny(missing_docs)]
pub trait RecordReader: Reader<Item = RecordBatch, Metadata = RecordMetadata> {}

impl<T> RecordReader for T where T: Reader<Item = RecordBatch, Metadata = RecordMetadata> + ?Sized {}

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
