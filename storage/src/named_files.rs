use std::sync::Arc;

use indexmap::IndexMap;

use crate::validation::index_files;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ListRequest,
    ObjectRef, ObjectStream, Result, async_trait,
};

/// A fixed, ordered namespace of opaque, nonempty, unique names mapped to byte objects.
/// Useful for provider manifests whose filenames differ from download locators.
#[derive(Clone, Debug)]
pub struct NamedFileAccessor<A> {
    inner: A,
    files: Arc<IndexMap<String, ObjectRef>>,
}

impl<A> NamedFileAccessor<A> {
    /// Construction performs no I/O.
    pub fn new(inner: A, files: Vec<(String, ObjectRef)>) -> Result<Self> {
        let files = index_files(files)?;
        Ok(Self {
            inner,
            files: Arc::new(files),
        })
    }
}

#[async_trait]
impl<A> Accessor for NamedFileAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            list: true,
            ..self.inner.capability()
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let object = self.files.get(request.object.locator()).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open_named_file",
                "filename is absent from manifest",
            )
        })?;
        self.inner
            .open(ByteReadRequest::new(object.clone()).with_options(request.options))
            .await
    }
}

#[async_trait]
impl<A> ByteAccessor for NamedFileAccessor<A>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    /// Names form a flat namespace; both recursion modes return manifest order.
    /// Only the empty root is a valid listing prefix. Listing performs no I/O.
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        if !request.prefix.locator().is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "list_named_files",
                "expected an empty root prefix",
            ));
        }
        let files = Arc::clone(&self.files);
        let len = files.len();
        Ok(Box::pin(futures::stream::iter((0..len).map(
            move |index| {
                let (name, _) = files.get_index(index).unwrap();
                Ok(ObjectRef::new(name.clone()))
            },
        ))))
    }
}
