use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ObjectRef,
    ObjectStream, Result, async_trait,
};
use opendal::Operator;

use super::HuggingFaceBuilder;
use crate::backend::{OpenDalAccessor, map_error};

/// Read repository-relative files through OpenDAL's Hugging Face service.
///
/// The builder selects repository type, ID, revision, root, authentication and
/// HTTP/Xet mode. Floating revisions are not snapshots. This adapter exposes
/// files rather than interpreting dataset scripts, configurations or splits.
#[derive(Clone, Debug)]
pub struct HuggingFaceAccessor {
    pub(super) inner: OpenDalAccessor,
}

impl HuggingFaceAccessor {
    /// Build the service without blocking a Tokio worker on implicit token-file I/O.
    /// OpenDAL retains its authentication and endpoint environment conventions.
    pub async fn new(builder: HuggingFaceBuilder) -> Result<Self> {
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
            inner: OpenDalAccessor::new(operator),
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
    /// Recursively list files under a repository-relative directory; empty selects root.
    /// Discovery follows Hub pagination order, which is not a stable dataset order.
    async fn list(&self, prefix: ObjectRef) -> Result<ObjectStream> {
        validate_path(prefix.locator(), true)?;
        self.inner.validate_directory(&prefix).await?;
        self.inner.list(prefix).await
    }
}

fn validate_path(path: &str, directory: bool) -> Result<()> {
    let components = if directory {
        path.strip_suffix('/').unwrap_or(path)
    } else {
        path
    };
    let operation = if directory { "list" } else { "open" };
    let invalid_path = |message| Error::new(ErrorKind::InvalidInput, operation, message);

    if components.is_empty() && !(directory && path.is_empty()) {
        return Err(invalid_path("path must not be empty"));
    }

    if path.trim() != path {
        return Err(invalid_path("path must not have surrounding whitespace"));
    }

    if components
        .split('/')
        .any(|part| part == "." || part == ".." || (!components.is_empty() && part.is_empty()))
    {
        return Err(invalid_path(
            "path must not contain empty or dot components",
        ));
    }

    if path.contains(['\\', '\0']) {
        return Err(invalid_path("path must not contain backslashes or NUL"));
    }

    Ok(())
}
