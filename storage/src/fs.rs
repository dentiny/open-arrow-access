use std::{
    num::NonZeroUsize,
    path::{Component, Path},
};

use open_arrow_access_core::{
    Accessor, BoxFuture, ByteAccessor, ByteReader, Error, ErrorKind, ObjectRef, Result,
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
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
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
        let operator = Operator::new(Fs::default().root(root))
            .map_err(|e| map_error("configure_fs", root, e))?;
        Ok(Self {
            inner: OpenDalAccessor::new(operator),
        })
    }

    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.inner = self.inner.with_chunk_size(chunk_size);
        self
    }
}

impl Accessor for FsAccessor {
    type Request = ObjectRef;
    type Reader = dyn ByteReader;

    /// Validate a root-relative file locator, then open it through the OpenDAL adapter.
    fn open(&self, object: ObjectRef) -> BoxFuture<'_, Result<Box<dyn ByteReader>>> {
        let path = object.locator();
        if path.is_empty()
            || Path::new(path)
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Box::pin(async {
                Err(Error::new(
                    ErrorKind::InvalidInput,
                    "open",
                    "expected a nonempty root-relative object path",
                ))
            });
        }
        self.inner.open(object)
    }
}

impl ByteAccessor for FsAccessor {}
