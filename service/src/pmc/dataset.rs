use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, Error, ErrorKind, ListRequest, ObjectStream,
    OrderRequirement, ReadRequest, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, async_trait,
};
use open_arrow_access_storage::OpenDalAccessor;

use super::{PmcAccessor, PmcRequest, request::Selection, schema};

/// Discover and read PMC article metadata, leaving content URLs for downstream sinks.
/// Clones share storage; each reader/partition owns its decoder state.
pub struct PmcDatasetAccessor<A> {
    storage: Arc<A>,
}

impl<A> Clone for PmcDatasetAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl<A> PmcDatasetAccessor<A> {
    /// Use a bucket-root byte accessor, including a local mirror for offline use.
    pub fn new(storage: A) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }
}

impl PmcDatasetAccessor<OpenDalAccessor> {
    /// Use the anonymous public PMC bucket.
    pub fn public() -> Result<Self> {
        Ok(Self::new(OpenDalAccessor::new(super::bucket_operator()?)))
    }
}

impl<A: ByteAccessor + 'static> PmcDatasetAccessor<A> {
    /// Stream selected metadata keys for inspection or custom composition.
    /// Explicit files require no listing; prefixes use recursive storage order.
    /// Errors from listing propagate without collecting the release into memory.
    pub async fn metadata_paths(&self, request: &PmcRequest) -> Result<ObjectStream> {
        request.validate()?;
        match &request.selection {
            Selection::Files(files) => Ok(stream::iter(files.clone().into_iter().map(Ok)).boxed()),
            Selection::Prefix(prefix) => {
                if !self.storage.capability().list {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "discover_pmc",
                        "storage does not support listing",
                    ));
                }
                let prefix = prefix.clone();
                let objects = self.storage.list(ListRequest::new(prefix.clone())).await?;
                Ok(objects
                    .and_then(move |object| {
                        let valid = object.locator().starts_with(&prefix);
                        futures::future::ready(if valid {
                            Ok(object)
                        } else {
                            Err(super::error::invalid_selection())
                        })
                    })
                    .try_filter(|object| {
                        futures::future::ready(object.locator().ends_with(".json"))
                    })
                    .boxed())
            }
        }
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for PmcDatasetAccessor<A> {
    type Request = PmcRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.storage.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    async fn open(&self, request: PmcRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for PmcDatasetAccessor<A> {
    async fn plan_scan(&self, request: PmcRequest) -> Result<ScanPlan> {
        // Validate projection before initiating any discovery I/O.
        let metadata = RecordMetadata {
            schema: request.options.resolve_projection(&schema())?.schema,
            source: None,
        };
        let objects = self.metadata_paths(&request).await?;
        let articles = PmcAccessor::with_default_decoder(self.storage.clone());
        let partitions = objects.map_ok(move |object| {
            let articles = articles.clone();
            let request = ReadRequest::new(object)
                .with_options(request.options.clone())
                .with_byte_options(request.byte_options);
            Box::pin(async move { articles.open(request).await }) as ScanPartition
        });
        Ok(ScanPlan::from_stream(metadata, partitions).with_ordering(self.capability().ordering))
    }
}
