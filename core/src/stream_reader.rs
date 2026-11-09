use std::pin::Pin;

use futures::{Stream, StreamExt};

use crate::{ByteMetadata, ByteReader, Bytes, Capability, Reader, Result, async_trait};

/// Adapt an owned stream of shared results into a sequential [`Reader`].
///
/// Metadata is available immediately; construction and metadata/capability
/// queries do not poll the stream. The stream need not implement [`Unpin`].
/// EOF and errors are terminal: the adapter drops the stream at either boundary
/// and returns EOF on subsequent calls. Dropping the reader also drops the stream;
/// the adapter starts no background tasks and performs no read-ahead.
///
/// Map upstream errors to [`crate::Error`] and apply format-specific conversions
/// before wrapping the stream. Items must match the supplied metadata: for
/// example, Arrow batches must have the schema declared by [`crate::RecordMetadata`].
/// A stream of those batches with that metadata automatically implements
/// [`crate::RecordReader`]. A byte stream with [`ByteMetadata`] implements
/// [`ByteReader`]; supply nonempty chunks as required by that contract.
/// The adapter exposes sequential reading only.
///
/// ```
/// use futures::stream;
/// use open_arrow_access_core::{Reader, StreamReader};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let stream = stream::iter([Ok(1_u64), Ok(2)]);
/// let mut reader = StreamReader::new(stream, "example");
/// assert_eq!(reader.metadata(), &"example");
/// assert_eq!(reader.next().await.unwrap(), Some(1));
/// assert_eq!(reader.next().await.unwrap(), Some(2));
/// assert_eq!(reader.next().await.unwrap(), None);
/// # }
/// ```
pub struct StreamReader<S, M> {
    stream: Option<Pin<Box<S>>>,
    metadata: M,
}

impl<S, M> StreamReader<S, M> {
    /// Own and pin the stream without polling it.
    pub fn new(stream: S, metadata: M) -> Self {
        Self {
            stream: Some(Box::pin(stream)),
            metadata,
        }
    }
}

#[async_trait]
impl<S, M, T> Reader for StreamReader<S, M>
where
    S: Stream<Item = Result<T>> + Send,
    M: Send + Sync + 'static,
    T: Send + 'static,
{
    type Item = T;
    type Metadata = M;

    fn capability(&self) -> Capability {
        Capability::sequential(true)
    }

    fn metadata(&self) -> &M {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<T>> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let result = stream.next().await.transpose();
        if !matches!(result, Ok(Some(_))) {
            self.stream = None;
        }
        result
    }
}

#[async_trait]
impl<S> ByteReader for StreamReader<S, ByteMetadata> where S: Stream<Item = Result<Bytes>> + Send {}
