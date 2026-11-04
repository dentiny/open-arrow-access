use std::ops::Range;

use futures::{future::BoxFuture, stream::BoxStream};

use crate::{Bytes, Error, ErrorKind, ObjectRef, RecordBatch, Result, SchemaRef};

/// A pull-based asynchronous reader. Metadata is available before the first item.
///
/// `Ok(None)` is EOF; EOF and errors are terminal. Dropping a reader stops this
/// library's work: implementations must not leave detached producer tasks running.
/// A future returned by `next` is Send. Cancelling a pending call need not make a
/// stateful decoder resumable; drop the reader after cancelling such a call.
pub trait Reader: Send {
    type Item: Send + 'static;
    type Metadata: Send + Sync + 'static;

    fn metadata(&self) -> &Self::Metadata;
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
pub trait ByteReader: Reader<Item = Bytes, Metadata = ByteMetadata> {
    fn capabilities(&self) -> ReaderCapabilities {
        ReaderCapabilities::default()
    }

    /// Read exactly [start, end) in the current byte representation. This does
    /// not move the sequential cursor. Invalid ranges and short reads are errors.
    /// A wrapper must delegate or explicitly remove range support; it must not
    /// advertise ranged access by repeatedly decoding from byte zero.
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
