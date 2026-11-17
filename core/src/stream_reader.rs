use std::pin::Pin;

use futures::{Stream, StreamExt};

use crate::{
    ByteMetadata, ByteReader, Bytes, Capability, ObjectRef, OrderRequirement, Reader,
    RecordCapability, RecordMetadata, Result, async_trait,
};

/// Adapt an owned stream of [`Result`]s into a sequential [`Reader`].
/// Pins without polling; EOF or errors drop the stream and make later reads return
/// EOF. Items must match the metadata; byte chunks must be nonempty. Use
/// [`StreamReader::records`] with an explicit ordering contract for Arrow records.
/// [`StreamReader::bytes`] retains byte metadata and attaches its source to lazy
/// errors. [`StreamReader::new`] supports generic item and metadata types.
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
pub struct StreamReader<S, M, C = Capability> {
    object: Option<ObjectRef>,
    capability: C,
    stream: Option<Pin<Box<S>>>,
    metadata: M,
}

impl<S, M> StreamReader<S, M> {
    /// Own and pin the stream without polling it.
    pub fn new(stream: S, metadata: M) -> Self {
        Self {
            object: None,
            capability: Capability {
                read: true,
                read_with_range: false,
                read_parallel: false,
                list: false,
            },
            stream: Some(Box::pin(stream)),
            metadata,
        }
    }
}

impl<S> StreamReader<S, ByteMetadata> {
    /// Construct a sequential byte stream, attaching its source to lazy errors.
    /// Construction does not poll the stream; metadata is retained after EOF or errors.
    pub fn bytes(stream: S, metadata: ByteMetadata) -> Self {
        let object = metadata.source.clone();
        Self::new(stream, metadata).with_object(object)
    }
}

impl<S, M, C> StreamReader<S, M, C> {
    /// Attach object context to lazy errors without changing their source chain.
    pub fn with_object(mut self, object: Option<ObjectRef>) -> Self {
        self.object = object;
        self
    }

    /// Attach an explicitly declared capability to a generic stream.
    pub fn with_capability<N>(self, capability: N) -> StreamReader<S, M, N> {
        StreamReader {
            object: self.object,
            stream: self.stream,
            metadata: self.metadata,
            capability,
        }
    }
}

impl<S> StreamReader<S, RecordMetadata, RecordCapability> {
    /// Construct a record stream with an explicit source ordering contract.
    pub fn records(stream: S, metadata: RecordMetadata, ordering: OrderRequirement) -> Self {
        Self::records_with_capability(stream, metadata, RecordCapability::sequential(ordering))
    }

    /// Construct a record stream retaining the caller's full capability declaration.
    /// The caller must ensure the stream supports the declared operations. Like
    /// [`Self::records`], this attaches `metadata.source` to lazy errors without
    /// polling the stream.
    pub fn records_with_capability(
        stream: S,
        metadata: RecordMetadata,
        capability: RecordCapability,
    ) -> Self {
        let object = metadata.source.clone();
        StreamReader::new(stream, metadata)
            .with_capability(capability)
            .with_object(object)
    }
}

#[async_trait]
impl<S, M, T, C> Reader for StreamReader<S, M, C>
where
    S: Stream<Item = Result<T>> + Send,
    M: Send + Sync + 'static,
    T: Send + 'static,
    C: Copy + Send + Sync + 'static,
{
    type Item = T;
    type Metadata = M;
    type Capability = C;

    fn capability(&self) -> C {
        self.capability
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
        result.map_err(|error| error.with_object(self.object.as_ref()))
    }
}

#[async_trait]
impl<S> ByteReader for StreamReader<S, ByteMetadata> where S: Stream<Item = Result<Bytes>> + Send {}

/// Read an owned buffer once, retaining its source and byte length after EOF.
/// Empty buffers start at EOF rather than emitting an empty byte chunk.
pub fn bytes_reader(bytes: Bytes, source: Option<ObjectRef>) -> impl ByteReader {
    let metadata = ByteMetadata {
        source,
        content_length: Some(bytes.len() as u64),
        version: None,
    };
    StreamReader::bytes(
        futures::stream::iter((!bytes.is_empty()).then_some(Ok(bytes))),
        metadata,
    )
}
