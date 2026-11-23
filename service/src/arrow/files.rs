//! Explicit file-format binding over a resolved namespace.
use crate::ResolvedFileSet;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, ReaderFactory, RecordReader,
};
use open_arrow_access_dataset::file_set::FileSetAccessor;
use open_arrow_access_file_format::FileAccessor;

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
    /// Build selections with [`crate::FileSetRequest`]. Both file-set types are
    /// exported by the service crate for scanning provider attachments.
    pub fn records<F>(self, format: F) -> FileSetAccessor<FileAccessor<Self, F>>
    where
        F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
    {
        FileSetAccessor::new(FileAccessor::from_format(self, format))
    }
}
