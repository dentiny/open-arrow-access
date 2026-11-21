//! Choose one typed attachment cell independently for each logical item.
use std::sync::Arc;

use open_arrow_access_core::{
    AttachmentCell, AttachmentCellAccessor, Error, ErrorKind, Result, async_trait,
};

type Condition = dyn Fn(&Error) -> bool + Send + Sync;

/// Prefer one attachment representation, falling back independently per cell.
///
/// Primary `Some(cell)` is returned unchanged, including empty binary content.
/// `None` or, by default, `NotFound` lazily invokes the alternate cell accessor.
/// Authentication, permissions, transport and decoding errors propagate. Both
/// absent representations return `None`; errors from the fallback are preserved.
/// [`Self::when`] changes which primary errors count as representation absence.
///
/// A cell contains its own data type, bytes and optional source identity. One
/// item's LaTeX cell can therefore be followed by another item's PDF cell. This
/// helper never opens a record reader, probes a batch, or changes other rows.
/// Providers resolve identity/version and decode content; callers assemble
/// selected cells into Arrow rows/batches afterwards.
///
/// Use [`Self::wrap`] rather than `AccessorExt::layer`: the stream-based `Layer`
/// contract returns readers, while this API returns exactly one optional cell.
/// Chain cell accessors for more than two representations.
///
/// ```
/// use open_arrow_access_core::AttachmentCellAccessor;
/// use open_arrow_access_layers::{AttachmentFallbackAccessor, AttachmentFallbackLayer};
///
/// fn prefer<A, F>(primary: A, alternate: F) -> AttachmentFallbackAccessor<A, F>
/// where
///     A: AttachmentCellAccessor,
///     A::Request: Clone,
///     F: AttachmentCellAccessor<Request = A::Request>,
/// {
///     AttachmentFallbackLayer::new(alternate).wrap(primary)
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

    /// Classify primary representation-absence errors. `None` always falls back.
    /// The callback may run concurrently and must not block.
    pub fn when(mut self, condition: impl Fn(&Error) -> bool + Send + Sync + 'static) -> Self {
        self.condition = Arc::new(condition);
        self
    }

    pub fn wrap<A>(&self, primary: A) -> AttachmentFallbackAccessor<A, F>
    where
        A: AttachmentCellAccessor,
        A::Request: Clone,
        F: AttachmentCellAccessor<Request = A::Request>,
    {
        AttachmentFallbackAccessor {
            primary,
            fallback: self.fallback.clone(),
            condition: self.condition.clone(),
        }
    }
}

pub struct AttachmentFallbackAccessor<A, F> {
    primary: A,
    fallback: Arc<F>,
    condition: Arc<Condition>,
}

#[async_trait]
impl<A, F> AttachmentCellAccessor for AttachmentFallbackAccessor<A, F>
where
    A: AttachmentCellAccessor,
    A::Request: Clone,
    F: AttachmentCellAccessor<Request = A::Request>,
{
    type Request = A::Request;

    async fn read_cell(&self, request: Self::Request) -> Result<Option<AttachmentCell>> {
        match self.primary.read_cell(request.clone()).await {
            Ok(Some(cell)) => Ok(Some(cell)),
            Ok(None) => self.fallback.read_cell(request).await,
            Err(error) if (self.condition)(&error) => self.fallback.read_cell(request).await,
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests;
