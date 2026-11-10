use std::sync::Arc;

use futures::stream::BoxStream;

use crate::{
    ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ListRequest, ObjectRef, Reader,
    Result, async_trait,
};

/// Factory for independently owned readers, with typed requests and outputs.
/// Use the re-exported `#[async_trait]` for `dyn` accessors with Send futures.
#[async_trait]
pub trait Accessor: Send + Sync {
    /// Selection and options needed to open one reader, interpreted by the accessor.
    type Request: Send;
    /// Reader interface returned by this accessor, including its item and metadata types.
    type Reader: Reader + ?Sized;

    /// Report this interface's effective operations without I/O; see [`Capability`].
    /// Opened readers report their own abilities via [`Reader::capability`].
    fn capability(&self) -> Capability;

    /// Open the accessor-defined selection and options in `request`.
    /// Opening may perform I/O and schema inspection; metadata is available on return
    /// without requiring all items to be loaded.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid, the input cannot be accessed,
    /// or opening/inspection fails. Failures encountered while consuming items
    /// are subsequently returned by [`Reader::next`].
    async fn open(&self, request: Self::Request) -> Result<Box<Self::Reader>>;
}

#[async_trait]
impl<A: Accessor + ?Sized> Accessor for Arc<A> {
    type Request = A::Request;
    type Reader = A::Reader;

    fn capability(&self) -> Capability {
        let inner = (**self).capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            read_parallel: inner.read_parallel,
            list: inner.list,
        }
    }

    async fn open(&self, request: Self::Request) -> Result<Box<Self::Reader>> {
        (**self).open(request).await
    }
}

pub type ObjectStream = BoxStream<'static, Result<ObjectRef>>;

/// Optional byte-accessor discovery boundary for directory and source adapters.
#[async_trait]
pub trait ByteAccessor: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> {
    /// Stream object references in accessor-defined prefix semantics and order.
    /// Apply recursion during discovery; nonrecursive requests must not descend
    /// into child directories. Implementations must advertise [`Capability::list`].
    ///
    /// # Errors
    ///
    /// Defaults to [`ErrorKind::Unsupported`]. Discovery failures may occur here
    /// or while consuming the stream.
    async fn list(&self, _request: ListRequest) -> Result<ObjectStream> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "list",
            "accessor does not support discovery",
        ))
    }
}

#[async_trait]
impl<A: ByteAccessor + ?Sized> ByteAccessor for Arc<A> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        (**self).list(request).await
    }
}
