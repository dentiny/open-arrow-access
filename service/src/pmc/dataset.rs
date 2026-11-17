use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ListRequest, ObjectStream, ReadRequest, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait,
};

use super::{PmcAccessor, schema};

/// Read a metadata directory ending in `/` recursively, or one `.json` object.
/// Paths are relative to the supplied bucket-root storage. Discovery streams in
/// storage order; a live directory listing is not an atomic release snapshot.
pub struct PmcDatasetAccessor<A> {
    storage: Arc<A>,
}

impl<A> PmcDatasetAccessor<A> {
    pub fn new(storage: A) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }
}

impl<A: ByteAccessor + 'static> PmcDatasetAccessor<A> {
    /// Stream metadata keys without collecting the directory into memory.
    /// An exact `.json` object requires no listing.
    /// Other locators must end in `/` to avoid matching sibling prefixes.
    pub async fn metadata_paths(&self, request: &ReadRequest) -> Result<ObjectStream> {
        if request.object.locator().ends_with(".json") {
            return Ok(stream::once(futures::future::ready(Ok(request.object.clone()))).boxed());
        }
        if !request.object.locator().ends_with('/') {
            return Err(super::error::invalid_metadata_path().with_object(Some(&request.object)));
        }
        Ok(self
            .storage
            .list(ListRequest::new(request.object.clone()))
            .await?
            .try_filter(|object| futures::future::ready(object.locator().ends_with(".json")))
            .boxed())
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for PmcDatasetAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        PmcAccessor::with_default_decoder(self.storage.clone()).capability()
    }

    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        // Validate the selection without listing or opening article documents.
        let path = request.object.locator();
        if !path.ends_with(".json") && !path.ends_with('/') {
            return Err(super::error::invalid_metadata_path().with_object(Some(&request.object)));
        }
        Ok(RecordMetadata {
            schema: request.options.resolve_projection(&schema())?.schema,
            source: None,
        })
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for PmcDatasetAccessor<A> {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let metadata = RecordMetadata {
            schema: request.options.resolve_projection(&schema())?.schema,
            source: None,
        };
        let objects = self.metadata_paths(&request).await?;
        let articles = PmcAccessor::with_default_decoder(self.storage.clone());
        let partitions = objects.map_ok(move |object| {
            let articles = articles.clone();
            let mut request = request.clone();
            request.object = object;
            Box::pin(async move { articles.open(request).await }) as ScanPartition
        });
        Ok(ScanPlan::from_stream(metadata, partitions).with_ordering(self.capability().ordering))
    }
}
