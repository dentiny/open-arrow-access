use super::{
    AttachmentOptions,
    format::{self, AttachmentRecords},
};
use crate::{
    Accessor, ByteAccessor, OrderRequirement, ReadRequest, RecordCapability, RecordMetadata,
    RecordReader, Result, ScanPlan, ScanPlanner, StreamReader, async_trait, into_stream,
};
use std::{marker::PhantomData, sync::Arc};

/// Reusable record accessor for explicitly selected provider attachments.
///
/// The byte accessor retains membership, credentials, and transport policy. Each
/// [`Accessor::open`] independently decodes the request's logical name with the
/// same format options, preserving its projection, batch size, and byte options.
/// Constructing or cloning this adapter performs no I/O; it shares the byte
/// accessor through an [`Arc`] without requiring the accessor itself to be `Clone`.
/// The format and compression adapters are bound once, without I/O. Native scan
/// planning forwards Parquet partitions or streaming-format chunks without an
/// extra schema open; binary objects remain indivisible documents.
///
/// Generic attachments retain `PreserveGlobal` ordering. Use ordered emission
/// when composing this accessor with `crate::crawl::CrawlAccessor` (feature `crawl`).
///
/// Use [`crate::open_attachment_source`] to bind one request immediately, or pass this
/// adapter to [`crate::BoundSource::bind`] or [`crate::PreparedSource::prepare`]
/// when the consumer needs an accessor rather than a bound source.
pub struct AttachmentAccessor<A> {
    inner: Arc<AttachmentRecords>,
    source: PhantomData<fn() -> A>,
}

impl<A: ByteAccessor + 'static> AttachmentAccessor<A> {
    /// Validate decoder configuration without accessing provider storage.
    ///
    /// # Errors
    /// Returns the same configuration and disabled-format errors as
    /// [`AttachmentOptions::validate`]. Request-specific errors are returned
    /// when opening a reader.
    pub fn new(accessor: A, options: AttachmentOptions) -> Result<Self> {
        Ok(Self {
            inner: format::records(accessor, options)?,
            source: PhantomData,
        })
    }
}

impl<A> Clone for AttachmentAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            source: PhantomData,
        }
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for AttachmentAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        let mut capability = self.inner.capability();
        capability.ordering = capability
            .ordering
            .combine(OrderRequirement::PreserveGlobal);
        capability
    }

    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        self.inner.metadata(request).await
    }

    async fn inspect_metadata(&self, request: ReadRequest) -> Result<Option<RecordMetadata>> {
        self.inner.inspect_metadata(request).await
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let reader = self.inner.open(request).await?;
        let metadata = reader.metadata().clone();
        let mut capability = reader.capability();
        capability.ordering = capability.ordering.combine(self.capability().ordering);
        Ok(Box::new(StreamReader::records_with_capability(
            into_stream(reader),
            metadata,
            capability,
        )))
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for AttachmentAccessor<A> {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        Ok(self
            .inner
            .plan_scan(request)
            .await?
            .require_ordering(self.capability().ordering))
    }
}
