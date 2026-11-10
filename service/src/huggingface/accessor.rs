use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectStream,
    Result, async_trait,
};
use open_arrow_access_storage::OpenDalAccessor;
use opendal::{OperationContext, Operator};

use super::map_error;

/// Read repository-relative files through OpenDAL's Hugging Face service.
///
/// Obtain this from `HuggingFaceService::repository` for explicit files.
/// `HuggingFaceService::dataset` pins Parquet config/split scans automatically.
/// Direct branch reads are not snapshots.
#[derive(Clone, Debug)]
pub struct HuggingFaceAccessor {
    pub(super) inner: OpenDalAccessor,
}

impl HuggingFaceAccessor {
    /// Build repository byte access using OpenDAL's configuration.
    pub(super) fn new(builder: opendal::services::Hf, context: OperationContext) -> Result<Self> {
        opendal::install_default();
        let operator = Operator::new(builder)
            .map_err(|error| map_error("configure_huggingface", "", error))?;
        Ok(Self {
            inner: OpenDalAccessor::new(operator.with_context(context)),
        })
    }

    /// Limit emitted bytes independently of per-open storage I/O chunk options.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.inner = self.inner.with_chunk_size(chunk_size);
        self
    }
}

#[async_trait]
impl Accessor for HuggingFaceAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            read_parallel: inner.read_parallel,
            list: true,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.inner.open(request).await
    }
}

#[async_trait]
impl ByteAccessor for HuggingFaceAccessor {
    /// List files under a repository-relative directory; empty selects root.
    /// Recursion follows the request and is enabled by default.
    /// Discovery follows Hub pagination order, which is not a stable dataset order.
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}
