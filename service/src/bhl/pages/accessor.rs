use super::super::BhlPageRecord;
use super::{BhlPageIdentity, page_schema};
use crate::FileSetRequest;
use arrow_array::StructArray;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, OrderRequirement, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, StreamReader, async_trait,
    read_object, scan_partition,
};
use std::{collections::HashMap, sync::Arc};

type MetadataProvider = dyn Fn(&BhlPageIdentity) -> Result<StructArray> + Send + Sync;

/// Read selected OCR pages with metadata explicitly supplied by the caller.
/// Source-table acquisition and joins are not performed by this accessor.
/// Projections that omit `data` perform no OCR storage reads and construct only
/// selected output columns. An empty projection still emits one row per page.
pub struct BhlPageAccessor<A> {
    storage: Arc<A>,
    snapshot_id: Arc<str>,
    metadata: Arc<MetadataProvider>,
    schema_metadata: HashMap<String, String>,
}
impl<A> BhlPageAccessor<A> {
    pub fn new(
        storage: A,
        snapshot_id: impl Into<String>,
        metadata: impl Fn(&BhlPageIdentity) -> Result<StructArray> + Send + Sync + 'static,
    ) -> Result<Self> {
        let snapshot_id = snapshot_id.into();
        if snapshot_id.trim().is_empty() {
            return Err(super::invalid("BHL pages require a nonempty snapshot_id"));
        }
        Ok(Self {
            storage: Arc::new(storage),
            snapshot_id: snapshot_id.into(),
            metadata: Arc::new(metadata),
            schema_metadata: HashMap::new(),
        })
    }
    /// Attach metadata snapshot provenance and reconciliation to emitted batches.
    pub fn with_schema_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.schema_metadata = metadata;
        self
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for BhlPageAccessor<A> {
    type Request = FileSetRequest;
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
    async fn open(&self, request: FileSetRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for BhlPageAccessor<A> {
    async fn plan_scan(&self, request: FileSetRequest) -> Result<ScanPlan> {
        if request.objects.is_empty() {
            return Err(super::invalid("empty BHL page selection"));
        }
        let schema = Arc::new(
            page_schema()
                .as_ref()
                .clone()
                .with_metadata(self.schema_metadata.clone()),
        );
        let projection = request.options.resolve_projection(&schema)?;
        let needs_data = projection.schema.field_with_name("data").is_ok();
        let prepared = request
            .objects
            .iter()
            .map(|object| {
                let identity = BhlPageIdentity::parse(object.locator())?;
                let metadata = (self.metadata)(&identity)?;
                Ok((identity, metadata))
            })
            .collect::<Result<Vec<_>>>()?;
        let metadata = RecordMetadata {
            schema: projection.schema,
            source: None,
        };
        let partitions = request
            .objects
            .into_iter()
            .zip(prepared)
            .map(|(object, (identity, source_metadata))| {
                let storage = self.storage.clone();
                let snapshot = self.snapshot_id.clone();
                let metadata = metadata.clone();
                scan_partition(async move {
                    let output_schema = metadata.schema.clone();
                    let batches = stream::once(async move {
                        let data = if needs_data {
                            read_object(
                                storage.as_ref(),
                                ByteReadRequest::new(object.clone())
                                    .with_options(request.byte_options),
                                "read_bhl_page",
                            )
                            .await?
                        } else {
                            Default::default()
                        };
                        let source_path =
                            format!("{}{}", super::super::OPEN_DATA_BUCKET_URL, object.locator());
                        let batch = BhlPageRecord {
                            entity_type: &identity.entity_type,
                            entity_id: identity.entity_id,
                            page_id: identity.page_id,
                            sequence: identity.sequence,
                            data: &data,
                            source_path: &source_path,
                            snapshot_id: &snapshot,
                            source_entry_index: None,
                            metadata: source_metadata,
                        }
                        .into_projected_batch(output_schema)?;
                        Ok(batch)
                    });
                    Ok(StreamReader::records(
                        batches,
                        metadata,
                        OrderRequirement::Reorderable,
                    ))
                })
            })
            .collect();
        Ok(ScanPlan::new(metadata, partitions).with_ordering(OrderRequirement::Reorderable))
    }
}
