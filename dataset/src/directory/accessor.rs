use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, ListRequest, OrderRequirement, ReadRequest, RecordReader,
    Result, ScanPlan, ScanPlanner, async_trait, list_files,
};

use super::DirectoryRequest;
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

    /// Use each selected format's native partitions under the shared executor budget.
    pub fn with_file_planning(mut self) -> Self
    where
        A: ScanPlanner<Request = ReadRequest> + 'static,
    {
        self.files = self.files.with_file_planning();
        self
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
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: DirectoryRequest) -> Result<Box<dyn RecordReader>> {
        self.files.open(self.resolve(request).await?).await
    }
}

impl<B, A> DirectoryAccessor<B, A>
where
    B: ByteAccessor,
{
    async fn resolve(&self, request: DirectoryRequest) -> Result<FileSetRequest> {
        let objects = list_files(
            &self.discovery,
            ListRequest::new(request.prefix).with_recursive(request.recursive),
            |relative| {
                Ok(!relative.is_empty()
                    && !relative.ends_with('/')
                    && request
                        .suffix
                        .as_ref()
                        .is_none_or(|suffix| relative.ends_with(suffix)))
            },
        )
        .await?;
        Ok(FileSetRequest {
            objects,
            options: request.options,
            byte_options: request.byte_options,
        })
    }
}

#[async_trait]
impl<B, A> ScanPlanner for DirectoryAccessor<B, A>
where
    B: ByteAccessor,
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    fn order_requirement(&self) -> OrderRequirement {
        self.files.order_requirement()
    }

    async fn plan_scan(&self, request: DirectoryRequest) -> Result<ScanPlan> {
        self.files.plan_scan(self.resolve(request).await?).await
    }
}
