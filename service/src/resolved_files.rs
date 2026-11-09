use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectStream,
    Result, async_trait,
};
use open_arrow_access_storage::NamedFileAccessor;

use crate::file_manifest::FileSpec;

/// A fixed file namespace with provider-defined metadata and independently owned readers.
#[derive(Clone, Debug)]
pub struct ResolvedFileSet<A, M> {
    accessor: NamedFileAccessor<A>,
    metadata: M,
}

impl<A, M> ResolvedFileSet<A, M> {
    pub fn new(accessor: NamedFileAccessor<A>, metadata: M) -> Self {
        Self { accessor, metadata }
    }

    pub(crate) fn from_files(
        accessor: A,
        files: impl IntoIterator<Item = FileSpec>,
        metadata: M,
    ) -> Result<Self> {
        let files = files
            .into_iter()
            .map(|file| (file.name, file.object))
            .collect();
        Ok(Self::new(
            NamedFileAccessor::new(accessor, files)?,
            metadata,
        ))
    }

    pub fn metadata(&self) -> &M {
        &self.metadata
    }
}

#[async_trait]
impl<A, M> Accessor for ResolvedFileSet<A, M>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
    M: Send + Sync,
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        self.accessor.capability()
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.accessor.open(request).await
    }
}

#[async_trait]
impl<A, M> ByteAccessor for ResolvedFileSet<A, M>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
    M: Send + Sync,
{
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.accessor.list(request).await
    }
}
