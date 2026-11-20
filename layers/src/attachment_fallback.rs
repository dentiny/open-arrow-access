//! Select an alternative attachment representation before committing to a stream.
use std::sync::Arc;

use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, Error, ErrorKind, Layer, RecordCapability, RecordReader, Result, StreamReader,
    async_trait, into_stream,
};

type Condition = dyn Fn(&Error) -> bool + Send + Sync;

/// Try another accessor when an attachment representation is unavailable.
///
/// By default only `NotFound` during opening or before the first batch selects
/// the fallback. Successful empty streams stay empty. Permission, transport and
/// decoding failures propagate unchanged. [`Self::when`] explicitly overrides
/// this classification; it does not change the stream commitment boundary.
///
/// Opening probes one batch and buffers it for the caller. After that batch is
/// selected, later errors never restart or switch streams: doing so could mix
/// representations or duplicate rows. Metadata describes the selected reader.
/// This is selection, not retry, and no schema conversion is performed.
///
/// Both accessors receive the same logical request. Configure representation,
/// storage, credentials and request resolution in the accessors themselves.
/// Compose layers for more than two candidates. Apply at one logical item's
/// boundary (e.g. one document), not a whole corpus requiring per-item fallback.
/// This layer does not implement `ScanPlanner` or change partition matching.
///
/// ```
/// use open_arrow_access_core::{Accessor, AccessorExt, RecordReader};
/// use open_arrow_access_layers::{AttachmentFallbackAccessor, AttachmentFallbackLayer};
///
/// fn prefer<A, F>(primary: A, alternate: F) -> AttachmentFallbackAccessor<A, F>
/// where
///     A: Accessor<Reader = dyn RecordReader>,
///     A::Request: Clone,
///     F: Accessor<Request = A::Request, Reader = dyn RecordReader>,
/// {
///     primary.layer(AttachmentFallbackLayer::new(alternate))
/// }
/// ```
pub struct AttachmentFallbackLayer<F> {
    fallback: Arc<F>,
    condition: Arc<Condition>,
}

impl<F> Clone for AttachmentFallbackLayer<F> {
    fn clone(&self) -> Self {
        Self {
            fallback: self.fallback.clone(),
            condition: self.condition.clone(),
        }
    }
}

impl<F> AttachmentFallbackLayer<F> {
    pub fn new(fallback: F) -> Self {
        Self {
            fallback: Arc::new(fallback),
            condition: Arc::new(|error| error.kind() == ErrorKind::NotFound),
        }
    }

    /// Select fallback for a caller-defined, classified unavailability error.
    /// The callback may run concurrently and must not block.
    pub fn when(mut self, condition: impl Fn(&Error) -> bool + Send + Sync + 'static) -> Self {
        self.condition = Arc::new(condition);
        self
    }
}

pub struct AttachmentFallbackAccessor<A, F> {
    primary: A,
    fallback: Arc<F>,
    condition: Arc<Condition>,
}

impl<A, F> Layer<A> for AttachmentFallbackLayer<F>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: Clone,
    F: Accessor<Request = A::Request, Reader = dyn RecordReader>,
{
    type Layered = AttachmentFallbackAccessor<A, F>;

    fn layer(&self, primary: A) -> Self::Layered {
        AttachmentFallbackAccessor {
            primary,
            fallback: self.fallback.clone(),
            condition: self.condition.clone(),
        }
    }
}

#[async_trait]
impl<A, F> Accessor for AttachmentFallbackAccessor<A, F>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: Clone,
    F: Accessor<Request = A::Request, Reader = dyn RecordReader>,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        let primary = self.primary.capability();
        let fallback = self.fallback.capability();
        RecordCapability {
            read: primary.read && fallback.read,
            read_with_range: false,
            read_parallel: primary.read_parallel && fallback.read_parallel,
            list: false,
            ordering: primary.ordering.combine(fallback.ordering),
        }
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let primary = match self.primary.open(request.clone()).await {
            Ok(reader) => probe(reader).await,
            Err(error) => Err(error),
        };
        match primary {
            Ok(reader) => Ok(reader),
            Err(error) if (self.condition)(&error) => {
                probe(self.fallback.open(request).await?).await
            }
            Err(error) => Err(error),
        }
    }
}

/// Resolve lazy first-read failures while preserving the selected batch exactly once.
async fn probe(mut reader: Box<dyn RecordReader>) -> Result<Box<dyn RecordReader>> {
    let Some(first) = reader.next().await? else {
        return Ok(reader);
    };
    let metadata = reader.metadata().clone();
    let ordering = reader.capability().ordering;
    let batches = stream::once(async { Ok(first) }).chain(into_stream(reader));
    Ok(Box::new(StreamReader::records(batches, metadata, ordering)))
}

#[cfg(test)]
mod tests;
