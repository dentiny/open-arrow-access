use std::{path::Path, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, ListRequest, ObjectRef, OrderRequirement, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, async_trait,
};

use super::{BhlShardReadRequest, validation};
use crate::{
    FileSetRequest,
    bhl::{
        BhlOpenData, BhlPageAccessor, BhlPageIdentity, error,
        metadata::{BhlMetadata, BhlPageMetadata},
        page_schema,
    },
};

type MetadataReport = dyn Fn(&BhlPageMetadata) -> Result<()> + Send + Sync;

/// Discover and read complete OCR item/part directories with joined metadata.
///
/// Listing is scoped to each requested directory and consumes all listing pages.
/// Only each directory's aggregate TXT object is excluded. Malformed identities,
/// empty selections and missing/conflicting metadata fail by default.
///
/// Implements the native scan planner: pass this accessor directly to
/// `CrawlAccessor::new`. The existing page planner retains projection pushdown,
/// its bounded metadata worker and one independent OCR partition per page.
pub struct BhlShardAccessor<A> {
    storage: Arc<A>,
    metadata: Arc<BhlMetadata>,
    snapshot_id: String,
    report: Arc<MetadataReport>,
}

impl<A> Clone for BhlShardAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            metadata: self.metadata.clone(),
            snapshot_id: self.snapshot_id.clone(),
            report: self.report.clone(),
        }
    }
}

impl<A: ByteAccessor + 'static> BhlShardAccessor<A> {
    /// Bind an already imported metadata snapshot without storage I/O.
    /// The metadata fingerprint is the default row snapshot label; it does not
    /// freeze the live OCR bucket. Explicit inventories can use BhlPageAccessor.
    pub fn from_metadata(storage: A, metadata: Arc<BhlMetadata>) -> Result<Self> {
        let snapshot_id = metadata.info.fingerprint.clone();
        if snapshot_id.trim().is_empty() {
            return Err(error::invalid_pages(
                "metadata fingerprint must not be blank",
            ));
        }
        Ok(Self {
            storage: Arc::new(storage),
            metadata,
            snapshot_id,
            report: Arc::new(error::require_page_metadata),
        })
    }

    /// Open a completed metadata database read-only on a blocking worker.
    /// Await BhlMetadata::create separately before opening readers. This never
    /// imports tables, creates a missing file, or writes to the snapshot.
    pub async fn from_database(storage: A, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let metadata = tokio::task::spawn_blocking(move || BhlMetadata::open(path))
            .await
            .map_err(error::metadata_worker)??;
        Self::from_metadata(storage, Arc::new(metadata))
    }

    /// Override the row snapshot label, retaining the metadata provenance fields.
    pub fn with_snapshot_id(mut self, snapshot_id: impl Into<String>) -> Result<Self> {
        let snapshot_id = snapshot_id.into();
        if snapshot_id.trim().is_empty() {
            return Err(error::invalid_pages("snapshot_id must not be blank"));
        }
        self.snapshot_id = snapshot_id;
        Ok(self)
    }

    /// Replace strict reconciliation with a caller policy, called for every page
    /// before its OCR read or emission. Ok retains it; an error stops the scan.
    pub fn with_report(
        mut self,
        report: impl Fn(&BhlPageMetadata) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.report = Arc::new(report);
        self
    }

    async fn discover_prefix(&self, prefix: &str) -> Result<Vec<ObjectRef>> {
        let group = validation::group(prefix)?;
        let aggregate = format!("{prefix}{group}.txt");
        let mut keys = Vec::new();
        for object in BhlOpenData::new(self.storage.clone())
            .discover(ListRequest::new(prefix))
            .await?
        {
            validation::object_in_shard(&object, prefix)?;
            if object.locator() == aggregate {
                continue;
            }
            BhlPageIdentity::parse(object.locator())?;
            keys.push(object);
        }
        validation::nonempty_shard(&keys, prefix)?;
        Ok(keys)
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for BhlShardAccessor<A> {
    type Request = BhlShardReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        let input = self.storage.capability();
        Capability {
            read: input.read && input.list,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    /// Inspect the fixed projected schema without listing or reading OCR data.
    async fn metadata(&self, request: Self::Request) -> Result<RecordMetadata> {
        let schema = Arc::new(
            page_schema()
                .as_ref()
                .clone()
                .with_metadata(self.metadata.schema_metadata()?),
        );
        Ok(RecordMetadata {
            schema: request.options.resolve_projection(&schema)?.schema,
            source: None,
        })
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for BhlShardAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        // Reject invalid projections before discovery performs any I/O.
        self.metadata(request.clone()).await?;
        let mut keys = Vec::new();
        let mut listings = Vec::with_capacity(request.prefixes.len());
        for prefix in &request.prefixes {
            listings.push(self.discover_prefix(prefix));
        }
        let mut listings = stream::iter(listings).buffered(request.listing_concurrency.get());
        while let Some(objects) = listings.try_next().await? {
            keys.extend(objects);
        }
        // One selection table/join worker for the complete directory batch.
        // Only keys are collected here; OCR bodies remain independent partitions.
        let report = self.report.clone();
        let pages = BhlPageAccessor::from_metadata(
            self.storage.clone(),
            self.snapshot_id.clone(),
            self.metadata.clone(),
            move |page| report(page),
        )?;
        pages
            .plan_scan(
                FileSetRequest::new(keys)
                    .with_options(request.options)
                    .with_byte_options(request.byte_options),
            )
            .await
    }
}
