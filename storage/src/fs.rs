use std::{
    num::NonZeroUsize,
    path::{Component, Path},
};

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReader, Capability, Error, ErrorKind, ObjectRef, Result,
    async_trait,
};
use opendal::{Operator, services::Fs};

use crate::backend::{OpenDalAccessor, map_error};

/// Asynchronous local-file access through OpenDAL's Fs service.
///
/// Object locators are relative to an absolute root. Files are expected to remain
/// unchanged while a reader is open. The root is a namespace, not a security
/// boundary: filesystem symlinks still follow the host's filesystem semantics.
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

    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.inner = self.inner.with_chunk_size(chunk_size);
        self
    }
}

#[async_trait]
impl Accessor for FsAccessor {
    type Request = ObjectRef;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn open(&self, object: ObjectRef) -> Result<Box<dyn ByteReader>> {
        let path = object.locator();
        if path.is_empty()
            || Path::new(path)
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "open",
                "expected a nonempty root-relative object path",
            ));
        }
        self.inner.open(object).await
    }
}

#[async_trait]
impl ByteAccessor for FsAccessor {}
