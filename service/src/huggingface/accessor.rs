use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ListRequest,
    ObjectStream, Result, async_trait,
};
use open_arrow_access_storage::OpenDalAccessor;
use opendal::{OperationContext, Operator};

use super::{map_error, validation::validate_path};

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
    /// Build the service without blocking a Tokio worker on implicit token-file I/O.
    /// OpenDAL retains its authentication and endpoint environment conventions.
    pub(super) async fn new(
        builder: opendal::services::Hf,
        context: OperationContext,
    ) -> Result<Self> {
        let operator = tokio::task::spawn_blocking(move || {
            opendal::install_default();
            Operator::new(builder).map_err(|error| {
                if error.kind() == opendal::ErrorKind::ConfigInvalid {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "configure_huggingface",
                        "invalid Hugging Face configuration",
                    )
                    .with_source(error)
                } else {
                    map_error("configure_huggingface", "", error)
                }
            })
        })
        .await
        .map_err(|error| {
            Error::new(
                ErrorKind::Io,
                "configure_huggingface",
                "Hugging Face initialization task failed",
            )
            .with_source(error)
        })??;
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
        validate_path(request.object.locator(), false)?;
        self.inner.open(request).await
    }
}

#[async_trait]
impl ByteAccessor for HuggingFaceAccessor {
    /// List files under a repository-relative directory; empty selects root.
    /// Recursion follows the request and is enabled by default.
    /// Discovery follows Hub pagination order, which is not a stable dataset order.
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        validate_path(request.prefix.locator(), true)?;
        self.inner.list(request).await
    }
}
