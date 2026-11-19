use std::sync::Arc;

use crate::{
    Accessor, AccessorExt, AttachmentAccessor, AttachmentOptions, ByteAccessor, EmissionPolicy,
    ReadOptionsProvider, RecordCapability, RecordMetadata, RecordReader, Result, ScanPlanner,
    async_trait, layers::UnorderedEmissionLayer,
};

use super::{CrawlOptions, CrawlResolver, discovery::ResolvingAccessor};

/// An integrated crawl with one executor and independently owned readers.
///
/// Accepts existing dataset/service planners directly, or a [`CrawlResolver`]
/// for service-specific snapshot/manifest discovery. Construction does no I/O.
/// Opening plans once, retains native partitions and inference samples, and starts
/// workers only on consumption. No source/client or byte-I/O settings are replaced.
///
/// This is an execution boundary: it intentionally exposes `Accessor`, not
/// `ScanPlanner`. Install partition policies on the source before constructing it;
/// install scan-wide sampling/limits outside it, using ordered emission when
/// reproducible selection is required.
pub struct CrawlAccessor<Q> {
    inner: Arc<dyn Accessor<Request = Q, Reader = dyn RecordReader>>,
}

impl<Q> Clone for CrawlAccessor<Q> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Q: ReadOptionsProvider + 'static> CrawlAccessor<Q> {
    pub fn new<A>(source: A, options: CrawlOptions) -> Self
    where
        A: ScanPlanner<Request = Q> + 'static,
    {
        let records = source.layer(options.evolution).layer(options.parallel_read);
        let mut inner: Arc<dyn Accessor<Request = Q, Reader = dyn RecordReader>> =
            match options.emission {
                EmissionPolicy::Ordered => Arc::new(records),
                EmissionPolicy::Unordered => Arc::new(records.layer(UnorderedEmissionLayer::new())),
            };
        if let Some(layer) = options.coalesce {
            inner = Arc::new(inner.layer(layer));
        }
        if let Some(layer) = options.prefetch {
            inner = Arc::new(inner.layer(layer));
        }
        Self { inner }
    }

    /// Resolve a typed service selection on each operation, then use the same
    /// execution path as an existing source. Resolvers should pin mutable release
    /// names once per resolution and retain that snapshot in the returned source.
    pub fn from_resolver<R>(resolver: R, options: CrawlOptions) -> Self
    where
        R: CrawlResolver<Request = Q> + 'static,
    {
        Self::new(ResolvingAccessor(resolver), options)
    }
}

#[async_trait]
impl<Q: Send + 'static> Accessor for CrawlAccessor<Q> {
    type Request = Q;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn metadata(&self, request: Q) -> Result<RecordMetadata> {
        self.inner.metadata(request).await
    }

    async fn inspect_metadata(&self, request: Q) -> Result<Option<RecordMetadata>> {
        self.inner.inspect_metadata(request).await
    }

    async fn open(&self, request: Q) -> Result<Box<dyn RecordReader>> {
        self.inner.open(request).await
    }
}

impl CrawlAccessor<crate::dataset::file_set::FileSetRequest> {
    /// Crawl selected logical names from a provider-bound attachment namespace.
    /// Format/compression binding, native file planning and execution share the
    /// same path for every provider. Construction performs no discovery or I/O;
    /// select one or more logical names with `FileSetRequest` when opening.
    /// Generic attachments require ordered emission; configure it in `options`.
    pub fn attachments<A: ByteAccessor + 'static>(
        source: A,
        attachment: AttachmentOptions,
        options: CrawlOptions,
    ) -> Result<Self> {
        Ok(Self::files(
            AttachmentAccessor::new(source, attachment)?,
            options,
        ))
    }

    /// Scan explicitly selected files, retaining the format's native partitions.
    /// For union-by-name or per-file normalization, configure a `FileSetAccessor`
    /// explicitly and pass it to `new` instead.
    pub fn files<A>(format: A, options: CrawlOptions) -> Self
    where
        A: ScanPlanner<Request = crate::ReadRequest> + 'static,
    {
        Self::new(
            crate::dataset::file_set::FileSetAccessor::new(format).with_file_planning(),
            options,
        )
    }
}

impl CrawlAccessor<crate::dataset::directory::DirectoryRequest> {
    /// Discover a directory/glob with the existing sorted-directory contract.
    /// Providers with streaming discovery should pass their planner to `new`.
    pub fn directory<B, A>(discovery: B, format: A, options: CrawlOptions) -> Self
    where
        B: crate::ByteAccessor + 'static,
        A: ScanPlanner<Request = crate::ReadRequest> + 'static,
    {
        Self::new(
            crate::dataset::directory::DirectoryAccessor::new(discovery, format)
                .with_file_planning(),
            options,
        )
    }
}
