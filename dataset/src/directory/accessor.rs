use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, Error, ErrorKind, ReadRequest, RecordReader, Result,
    async_trait,
};

use super::{DirectoryRequest, path::is_root_relative};
use crate::{FileSetAccessor, FileSetRequest};

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
        Capability {
            read: self.discovery.capability().list && self.files.capability().read,
            read_with_range: false,
            list: false,
        }
    }

    async fn open(&self, request: DirectoryRequest) -> Result<Box<dyn RecordReader>> {
        let prefix = request.prefix.locator();
        if !is_root_relative(prefix) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "open_directory",
                "expected a root-relative directory prefix without dot components",
            ));
        }
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", prefix.trim_end_matches('/'))
        };
        let mut listing = self.discovery.list(request.prefix).await?;
        let mut objects = Vec::new();
        while let Some(object) = listing.try_next().await? {
            let Some(relative) = object.locator().strip_prefix(&prefix) else {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "open_directory",
                    "listing returned an object outside its directory prefix",
                ));
            };
            if !is_root_relative(relative) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "open_directory",
                    "listing returned a rooted or dot-component locator",
                ));
            }
            if relative.is_empty() || relative.ends_with('/') {
                continue;
            }
            if !request.recursive && relative.contains('/') {
                continue;
            }
            if !request.include_hidden && relative.split('/').any(|part| part.starts_with('.')) {
                continue;
            }
            if request
                .suffix
                .as_ref()
                .is_some_and(|suffix| !relative.ends_with(suffix))
            {
                continue;
            }
            objects.push(object);
        }
        objects.sort_unstable_by(|a, b| a.locator().cmp(b.locator()));
        objects.dedup();
        self.files
            .open(FileSetRequest {
                objects,
                options: request.options,
                byte_options: request.byte_options,
            })
            .await
    }
}
