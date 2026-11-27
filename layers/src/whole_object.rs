use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Layer, ListRequest,
    ObjectStream, Result, async_trait,
};

/// Opt in to sequential whole-object consumption at the byte-storage boundary.
///
/// At I/O concurrency one, discard the request's I/O splitting size so a global
/// chunk setting cannot force range splitting or eager length probes. Explicit
/// parallel I/O retains its options. Output chunk sizes are unaffected.
/// Both `open` and `open_sequential` use the backend's sequential opening path;
/// this accessor does not promise random access. Backend-specific metadata
/// fallbacks may still perform probes.
///
/// Install **below** decompression so its backing `open` also uses this policy:
/// ```
/// use open_arrow_access_core::{AccessorExt, ByteAccessor};
/// use open_arrow_access_layers::WholeObjectLayer;
///
/// fn whole_objects<A: ByteAccessor>(storage: A) -> impl ByteAccessor {
///     storage.layer(WholeObjectLayer)
/// }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct WholeObjectLayer;

/// Byte accessor decorated by [`WholeObjectLayer`], forwarding discovery.
#[derive(Clone, Debug)]
pub struct WholeObjectAccessor<A> {
    inner: A,
}

impl<A: ByteAccessor> Layer<A> for WholeObjectLayer {
    type Layered = WholeObjectAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        WholeObjectAccessor { inner }
    }
}

#[async_trait]
impl<A: ByteAccessor> Accessor for WholeObjectAccessor<A> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read_with_range: false,
            read_parallel: false,
            ..self.inner.capability()
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.open_sequential(request).await
    }

    async fn open_sequential(&self, mut request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        if request.options.io_concurrency.get() == 1 {
            request.options.io_chunk_size = None;
        }
        self.inner.open_sequential(request).await
    }
}

#[async_trait]
impl<A: ByteAccessor> ByteAccessor for WholeObjectAccessor<A> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}
