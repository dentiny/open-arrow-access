use crate::{
    Accessor, ByteReadRequest, ByteReader, Capability, DocumentAccessor, DocumentDecoder,
    RecordReader, Result, async_trait,
};

/// A single-document accessor whose decoder has a default configuration.
///
/// Providers can alias this type and implement only [`DocumentDecoder`] plus
/// [`Default`] for their decoder, retaining a one-argument `new(storage)` entry
/// point. For explicitly configured decoders, use [`DocumentAccessor::new`].
/// Both entry points share projection, metadata, ownership and lazy input opening.
pub struct DefaultDocumentAccessor<B, D> {
    inner: DocumentAccessor<B, D>,
}

impl<B, D: Default> DefaultDocumentAccessor<B, D> {
    /// Configure the default decoder without opening storage or reading input.
    pub fn new(storage: B) -> Self {
        Self {
            inner: DocumentAccessor::new(storage, D::default()),
        }
    }
}

impl<B, D> Clone for DefaultDocumentAccessor<B, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<B, D> Accessor for DefaultDocumentAccessor<B, D>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
    D: DocumentDecoder + 'static,
    for<'a> ByteReadRequest: From<&'a D::Request>,
{
    type Request = D::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.inner.open(request).await
    }
}
