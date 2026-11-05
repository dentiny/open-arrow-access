use std::{
    num::NonZeroUsize,
    path::{Component, Path},
};

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ObjectRef,
    ObjectStream, Result, async_trait,
};
use opendal::{Operator, services::Fs};

use crate::backend::{OpenDalAccessor, map_error};

/// Asynchronous local-file access through OpenDAL's Fs service.
///
/// Object locators are relative to an absolute root. Files are expected to remain
/// unchanged while a reader is open. The root is a namespace, not a security
/// boundary: filesystem symlinks still follow the host's filesystem semantics.
/// Locators with leading or trailing whitespace are rejected because OpenDAL
/// trims it, which could select a different file. Listing recursively yields
/// files in backend order; an empty prefix selects the configured root.
#[derive(Clone, Debug)]
pub struct FsAccessor {
    inner: OpenDalAccessor,
}

impl FsAccessor {
    /// Initialize OpenDAL's filesystem service without blocking a Tokio worker.
    ///
    /// OpenDAL may inspect/create the root and resolve filesystem symlinks. Its
    /// synchronous builder runs on Tokio's blocking pool when this future is
    /// polled. The caller must run this method inside a Tokio runtime.
    pub async fn new(root: impl AsRef<Path> + Send) -> Result<Self> {
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "configure_fs",
                "root must be absolute",
            ));
        }
        let root = root.to_str().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "configure_fs",
                "root must be valid UTF-8",
            )
        })?;
        let root = root.to_owned();
        let operator = tokio::task::spawn_blocking(move || {
            Operator::new(Fs::default().root(&root))
                .map_err(|e| map_error("configure_fs", &root, e))
        })
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::Io,
                "configure_fs",
                "filesystem initialization task failed",
            )
            .with_source(e)
        })??;
        Ok(Self {
            inner: OpenDalAccessor::new(operator),
        })
    }

    /// Limit bytes emitted by `next()`, independently of storage I/O splitting.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.inner = self.inner.with_chunk_size(chunk_size);
        self
    }
}

#[async_trait]
impl Accessor for FsAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let path = request.object.locator();
        if path.is_empty()
            || path.trim() != path
            || Path::new(path)
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "open",
                "expected a nonempty root-relative object path without leading or trailing whitespace",
            ));
        }
        self.inner.open(request).await
    }
}

#[async_trait]
impl ByteAccessor for FsAccessor {
    async fn list(&self, prefix: ObjectRef) -> Result<ObjectStream> {
        let path = prefix.locator();
        if path.trim() != path
            || Path::new(path)
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "list",
                "expected a root-relative directory path without surrounding whitespace",
            ));
        }
        self.inner.validate_directory(&prefix).await?;
        self.inner.list(prefix).await
    }
}
