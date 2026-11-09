use std::sync::Arc;

use indexmap::{IndexMap, map::Entry};

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ListRequest,
    ObjectRef, ObjectStream, Result, async_trait,
};

/// Validate an opaque file namespace without constructing an accessor or performing I/O.
/// Empty names and duplicate names fail; order, paths and download locators are not interpreted.
pub fn validate_file_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<()> {
    index_files(names.into_iter().map(|name| (name, ()))).map(|_| ())
}

/// A fixed, ordered namespace of names mapped to caller-supplied byte objects.
/// Useful for provider manifests whose filenames differ from download locators.
#[derive(Clone, Debug)]
pub struct NamedFileAccessor<A> {
    inner: A,
    files: Arc<IndexMap<String, ObjectRef>>,
}

impl<A> NamedFileAccessor<A> {
    /// Names are opaque and must be nonempty and unique. Construction performs no I/O.
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

// Both borrowed-name validation and owned accessor construction use this path.
// Build the ordered index while validating, without a second hash table/pass.
fn index_files<K, V>(files: impl IntoIterator<Item = (K, V)>) -> Result<IndexMap<K, V>>
where
    K: AsRef<str> + Eq + std::hash::Hash,
{
    let files = files.into_iter();
    let mut indexed = IndexMap::with_capacity(files.size_hint().0);
    for (name, object) in files {
        if name.as_ref().is_empty() {
            return Err(invalid_names());
        }
        match indexed.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(object);
            }
            Entry::Occupied(_) => return Err(invalid_names()),
        }
    }
    Ok(indexed)
}

fn invalid_names() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "configure_named_files",
        "empty or duplicate filename",
    )
}
