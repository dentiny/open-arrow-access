//! Explicit routing from advertised Gutenberg URLs to a caller-owned mirror.

use super::validation::mirror_path;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadRequest, ByteReader, Capability, Result,
    async_trait,
};

/// Read advertised Gutenberg file URLs from a caller-configured mirror.
///
/// `prefix` is relative to the wrapped accessor's root. The mirror must expose
/// the main digit tree and generated files beneath `cache/epub/`. URI aliases
/// are resolved without changing the original RDF or Arrow metadata fields.
/// Missing files and unsupported URLs remain errors; there is no fallback.
/// Discovery is not supported. Read options and the backend's sequential-open
/// and metadata paths are preserved.
pub struct GutenbergMirror<A> {
    inner: A,
    prefix: String,
}

impl<A> GutenbergMirror<A> {
    pub fn new(inner: A, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into().trim_end_matches('/').to_owned(),
        }
    }

    fn map_request(&self, mut request: ByteReadRequest) -> Result<ByteReadRequest> {
        let path = mirror_path(request.object.locator())?;
        request.object = if self.prefix.is_empty() {
            path
        } else {
            format!("{}/{path}", self.prefix)
        }
        .into();
        Ok(request)
    }
}

#[async_trait]
impl<A> Accessor for GutenbergMirror<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        capability.list = false;
        capability
    }

    async fn metadata(&self, request: ByteReadRequest) -> Result<ByteMetadata> {
        self.inner.metadata(self.map_request(request)?).await
    }

    async fn inspect_metadata(&self, request: ByteReadRequest) -> Result<Option<ByteMetadata>> {
        self.inner
            .inspect_metadata(self.map_request(request)?)
            .await
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.inner.open(self.map_request(request)?).await
    }

    async fn open_sequential(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.inner.open_sequential(self.map_request(request)?).await
    }
}

#[async_trait]
impl<A> ByteAccessor for GutenbergMirror<A> where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>
{
}
