use crate::{
    Accessor, ReadOptionsProvider, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPlan, ScanPlanner, async_trait,
};

/// A service-resolved selection, including its bound storage, credentials,
/// decoder and snapshot. Constructing this value need not open any record files.
/// The accessor may stream discovery rather than collect a file list.
pub struct ResolvedCrawl<A: Accessor> {
    pub accessor: A,
    pub request: A::Request,
}

/// Service-specific discovery only; execution belongs to [`super::CrawlAccessor`].
///
/// Resolve sharding, manifests, signed URLs and immutable release/revision IDs
/// here. Return an existing FileSet/Directory/service planner with its native
/// partitions intact. Discovery may perform metadata I/O, but must not download
/// all records or start a nested executor. No extra schema inspection is required.
#[async_trait]
pub trait CrawlResolver: Send + Sync {
    type Request: ReadOptionsProvider;
    type Source: ScanPlanner<Request: ReadOptionsProvider> + 'static;

    /// Conservative contract for every possible resolved selection; resolving
    /// cannot weaken a returned source's global ordering requirement.
    fn capability(&self) -> RecordCapability;

    async fn resolve(&self, request: Self::Request) -> Result<ResolvedCrawl<Self::Source>>;
}

pub(super) struct ResolvingAccessor<R>(pub R);

impl<R: CrawlResolver> ResolvingAccessor<R> {
    async fn resolve(&self, mut request: R::Request) -> Result<ResolvedCrawl<R::Source>> {
        // Preserve caller options, including projection rewritten for evolution,
        // even if the provider constructs its selected request with defaults.
        let options = request.read_options().clone();
        let byte_options = request.byte_read_options_mut().copied();
        let mut resolved = self.0.resolve(request).await?;
        *resolved.request.read_options_mut() = options;
        if let Some(options) = byte_options
            && let Some(target) = resolved.request.byte_read_options_mut()
        {
            *target = options;
        }
        Ok(resolved)
    }
}

#[async_trait]
impl<R: CrawlResolver> Accessor for ResolvingAccessor<R> {
    type Request = R::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.0.capability()
    }

    async fn metadata(&self, request: R::Request) -> Result<RecordMetadata> {
        let selected = self.resolve(request).await?;
        selected.accessor.metadata(selected.request).await
    }

    async fn open(&self, request: R::Request) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}

#[async_trait]
impl<R: CrawlResolver> ScanPlanner for ResolvingAccessor<R> {
    async fn plan_scan(&self, request: R::Request) -> Result<ScanPlan> {
        let selected = self.resolve(request).await?;
        let ordering = self
            .capability()
            .ordering
            .combine(selected.accessor.capability().ordering);
        Ok(selected
            .accessor
            .plan_scan(selected.request)
            .await?
            .require_ordering(ordering))
    }
}
