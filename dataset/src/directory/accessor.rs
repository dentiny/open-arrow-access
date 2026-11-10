use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, Error, ErrorKind, ListRequest, ReadRequest, RecordReader,
    Result, async_trait, list_files,
};

use super::{
    DirectoryRequest,
    path::{is_root_relative, normalize_prefix},
};
use crate::file_set::{FileSetAccessor, FileSetRequest};

/// Discover files through a byte accessor and read them through a supplied format.
///
/// Listing is collected before opening the first file to establish lexicographic
/// order. Memory is proportional to the selected file count; file contents and
/// record batches are read lazily and sequentially.
#[derive(Clone, Debug)]
pub struct DirectoryAccessor<B, A> {
    discovery: B,
    files: FileSetAccessor<A>,
}

impl<B, A> DirectoryAccessor<B, A> {
    pub fn new(discovery: B, format: A) -> Self {
        Self {
            discovery,
            files: FileSetAccessor::new(format),
        }
    }
}

#[async_trait]
impl<B, A> Accessor for DirectoryAccessor<B, A>
where
    B: ByteAccessor,
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Request = DirectoryRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability::sequential(self.discovery.capability().list && self.files.capability().read)
    }

    async fn open(&self, request: DirectoryRequest) -> Result<Box<dyn RecordReader>> {
        let prefix = normalize_prefix(request.prefix.locator()).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "open_directory",
                "expected a root-relative directory prefix without dot components",
            )
        })?;
        let objects = list_files(
            &self.discovery,
            ListRequest::new(prefix).with_recursive(request.recursive),
            |relative| {
                if !is_root_relative(relative) {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "open_directory",
                        "listing returned a rooted or dot-component locator",
                    ));
                }
                Ok(!relative.is_empty()
                    && !relative.ends_with('/')
                    && request
                        .suffix
                        .as_ref()
                        .is_none_or(|suffix| relative.ends_with(suffix)))
            },
        )
        .await?;
        self.files
            .open(FileSetRequest {
                objects,
                options: request.options,
                byte_options: request.byte_options,
            })
            .await
    }
}
