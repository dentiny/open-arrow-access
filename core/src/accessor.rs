use std::sync::Arc;

use futures::stream::BoxStream;

use crate::{ByteReadRequest, ByteReader, ListRequest, ObjectRef, Reader, Result, async_trait};

/// Factory for independently owned readers, with typed requests and outputs.
/// Use the re-exported `#[async_trait]` for `dyn` accessors with Send futures.
#[async_trait]
pub trait Accessor: Send + Sync {
    /// Selection and options needed to open one reader, interpreted by the accessor.
    type Request: Send;
    /// Reader interface returned by this accessor, including its item and metadata types.
    type Reader: Reader + ?Sized;

    /// Report effective operations without I/O using [`crate::Capability`] or
    /// [`crate::RecordCapability`], according to the reader representation.
    /// Opened readers report their own abilities via [`Reader::capability`].
    fn capability(&self) -> <Self::Reader as Reader>::Capability;

    /// Return the metadata exposed by `open(request)`, without consuming output items.
    ///
    /// For unchanged input and configuration, this must equal the opened reader's
    /// metadata, including projection and source identity. The compatibility default
    /// opens and immediately drops a reader; it inherits any opening side effects.
    /// Implementations should override it to inspect only bounded inference samples
    /// or format metadata, without starting background work or retaining readers.
    async fn metadata(&self, request: Self::Request) -> Result<<Self::Reader as Reader>::Metadata>
    where
        <Self::Reader as Reader>::Metadata: Clone,
    {
        Ok(self.open(request).await?.metadata().clone())
    }

    /// Inspect metadata, returning `None` only for a proven empty input with no schema.
    /// Preserve `Some` for unknown row presence, explicit schemas, and JSON `{}` rows.
    /// The default wraps `metadata` in `Some` and inherits its fallback.
    async fn inspect_metadata(
        &self,
        request: Self::Request,
    ) -> Result<Option<<Self::Reader as Reader>::Metadata>>
    where
        <Self::Reader as Reader>::Metadata: Clone,
    {
        Ok(Some(self.metadata(request).await?))
    }

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

    fn capability(&self) -> <Self::Reader as Reader>::Capability {
        (**self).capability()
    }

    async fn metadata(&self, request: Self::Request) -> Result<<Self::Reader as Reader>::Metadata>
    where
        <Self::Reader as Reader>::Metadata: Clone,
    {
        (**self).metadata(request).await
    }

    async fn inspect_metadata(
        &self,
        request: Self::Request,
    ) -> Result<Option<<Self::Reader as Reader>::Metadata>>
    where
        <Self::Reader as Reader>::Metadata: Clone,
    {
        (**self).inspect_metadata(request).await
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
    /// into child directories. Implementations must advertise [`crate::Capability::list`].
    ///
    /// # Errors
    ///
    /// Defaults to [`crate::ErrorKind::Unsupported`]. Discovery failures may occur here
    /// or while consuming the stream.
    async fn list(&self, _request: ListRequest) -> Result<ObjectStream> {
        Err(crate::error::default_listing_unsupported())
    }
}

#[async_trait]
impl<A: ByteAccessor + ?Sized> ByteAccessor for Arc<A> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        (**self).list(request).await
    }
}
