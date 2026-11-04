use std::sync::Arc;

use futures::{future::BoxFuture, stream::BoxStream};

use crate::{ByteReader, Error, ErrorKind, ObjectRef, Reader, Result};

/// Factory for readers. The request and reader types keep bytes and Arrow records
/// statically distinct while sharing the same opening and layering contract.
#[deny(missing_docs)]
pub trait Accessor: Send + Sync {
    /// Selection and options needed to open one reader, interpreted by the accessor.
    type Request: Send;
    /// Reader interface returned by this accessor, including its item and metadata types.
    type Reader: Reader + ?Sized;

    /// Open the input selected by `request` and return an independently owned reader.
    ///
    /// Opening may perform I/O and schema inspection. Reader metadata must be
    /// available when this future completes; returning a reader does not require
    /// loading all of its items. The request's locator and options are interpreted
    /// by the concrete accessor rather than by the shared protocol.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid, the input cannot be accessed,
    /// or opening/inspection fails. Failures encountered while consuming items
    /// are subsequently returned by [`Reader::next`].
    fn open(&self, request: Self::Request) -> BoxFuture<'_, Result<Box<Self::Reader>>>;
}

impl<A: Accessor + ?Sized> Accessor for Arc<A> {
    type Request = A::Request;
    type Reader = A::Reader;

    /// Delegate opening to the shared accessor without sharing the returned reader.
    fn open(&self, request: Self::Request) -> BoxFuture<'_, Result<Box<Self::Reader>>> {
        (**self).open(request)
    }
}

pub type ObjectStream = BoxStream<'static, Result<ObjectRef>>;

/// Optional byte-accessor discovery boundary, reserved for directory/source PRs.
/// The initial filesystem adapter does not expose directory scans.
#[deny(missing_docs)]
pub trait ByteAccessor: Accessor<Request = ObjectRef, Reader = dyn ByteReader> {
    /// Report whether this accessor implements object discovery through [`Self::list`].
    ///
    /// The default is false. This is an ability report, not a guarantee that any
    /// particular listing request will succeed or that its results form a snapshot.
    fn can_list(&self) -> bool {
        false
    }

    /// Discover object references under an accessor-defined `prefix`.
    ///
    /// The accessor defines prefix interpretation, recursion and listing order.
    /// Objects are returned as a stream so discovery need not collect all results
    /// in memory. Additional I/O errors may occur while consuming that stream.
    ///
    /// # Errors
    ///
    /// The default returns [`ErrorKind::Unsupported`]. Implementations also report
    /// invalid selections, access failures and failures starting discovery.
    fn list(&self, _prefix: ObjectRef) -> BoxFuture<'_, Result<ObjectStream>> {
        Box::pin(async {
            Err(Error::new(
                ErrorKind::Unsupported,
                "list",
                "accessor does not support discovery",
            ))
        })
    }
}

impl<A: ByteAccessor + ?Sized> ByteAccessor for Arc<A> {
    /// Forward the discovery capability of the shared accessor.
    fn can_list(&self) -> bool {
        (**self).can_list()
    }

    /// Forward discovery while preserving its lazy stream and errors.
    fn list(&self, prefix: ObjectRef) -> BoxFuture<'_, Result<ObjectStream>> {
        (**self).list(prefix)
    }
}
