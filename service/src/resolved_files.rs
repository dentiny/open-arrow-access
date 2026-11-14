use std::sync::Arc;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectStream,
    ReadRequest, ReaderFactory, RecordReader, Result, async_trait,
};
use open_arrow_access_dataset::file_set::FileSetAccessor;
use open_arrow_access_file_format::FileAccessor;
use open_arrow_access_storage::NamedFileAccessor;

use crate::{ManifestRecords, file_manifest::FileSpec};

/// A fixed file namespace with provider-defined metadata and independently owned readers.
#[derive(Clone, Debug)]
pub struct ResolvedFileSet<A, M> {
    accessor: NamedFileAccessor<A>,
    metadata: M,
    // Aligned with the immutable named-file order; empty means all sizes are unknown.
    sizes: Arc<[Option<u64>]>,
}

impl<A, M> ResolvedFileSet<A, M> {
    pub fn new(accessor: NamedFileAccessor<A>, metadata: M) -> Self {
        Self {
            accessor,
            metadata,
            sizes: Arc::from([]),
        }
    }

    pub(crate) fn from_files(
        accessor: A,
        files: impl IntoIterator<Item = FileSpec>,
        metadata: M,
    ) -> Result<Self> {
        let (files, sizes): (Vec<_>, Vec<_>) = files
            .into_iter()
            .map(|file| {
                let size = file.size.or(file.content_length);
                ((file.name, file.object), size)
            })
            .unzip();
        let sizes = if sizes.iter().all(Option::is_none) {
            Vec::new()
        } else {
            sizes
        };
        Ok(Self {
            accessor: NamedFileAccessor::new(accessor, files)?,
            metadata,
            sizes: sizes.into(),
        })
    }

    /// Read the fixed file namespace as Arrow rows without accessing storage.
    ///
    /// Columns are `file_name` (non-null Utf8) and `size` (nullable UInt64).
    /// Known sizes are supplied by the provider at resolution, not verified by
    /// stat/HEAD calls. Unknown sizes remain null, and zero is retained as zero.
    /// Names and aliases stay in manifest order. Provider metadata remains
    /// available through `metadata()`; download locators are not output columns.
    /// The returned accessor owns a snapshot and remains usable after `self` drops.
    pub fn manifest_records(&self) -> ManifestRecords {
        ManifestRecords::new(
            self.accessor
                .names()
                .enumerate()
                .map(|(index, name)| (name.to_owned(), self.sizes.get(index).copied().flatten()))
                .collect(),
        )
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

impl<A, M> ResolvedFileSet<A, M>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
    M: Send + Sync + 'static,
{
    /// Bind an explicit Arrow format for sequential reads or scan planning.
    /// [`FileSetAccessor`] selects logical names in caller order, including duplicates;
    /// each selected file opens an independent byte reader on demand.
    /// Call `with_file_planning()` on the result when the format supports native
    /// partitions, to combine file-internal and cross-file parallelism.
    pub fn records<F>(self, format: F) -> FileSetAccessor<FileAccessor<Self, F>>
    where
        F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
    {
        FileSetAccessor::new(FileAccessor::from_format(self, format))
    }
}
