use std::sync::Arc;

use futures::{future::BoxFuture, stream::BoxStream};

use crate::{ByteReader, Error, ErrorKind, ObjectRef, Reader, Result};

/// Factory for readers. The request and reader types keep bytes and Arrow records
/// statically distinct while sharing the same opening and layering contract.
pub trait Accessor: Send + Sync {
    type Request: Send;
    type Reader: Reader + ?Sized;

    fn open(&self, request: Self::Request) -> BoxFuture<'_, Result<Box<Self::Reader>>>;
}

impl<A: Accessor + ?Sized> Accessor for Arc<A> {
    type Request = A::Request;
    type Reader = A::Reader;

    fn open(&self, request: Self::Request) -> BoxFuture<'_, Result<Box<Self::Reader>>> {
        (**self).open(request)
    }
}

pub type ObjectStream = BoxStream<'static, Result<ObjectRef>>;

/// Optional byte-accessor discovery boundary, reserved for directory/source PRs.
/// The initial filesystem adapter does not expose directory scans.
pub trait ByteAccessor: Accessor<Request = ObjectRef, Reader = dyn ByteReader> {
    fn can_list(&self) -> bool {
        false
    }

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
    fn can_list(&self) -> bool {
        (**self).can_list()
    }

    fn list(&self, prefix: ObjectRef) -> BoxFuture<'_, Result<ObjectStream>> {
        (**self).list(prefix)
    }
}
