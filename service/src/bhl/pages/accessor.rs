use super::{BhlPageIdentity, page_schema};
use crate::FileSetRequest;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, OrderRequirement, RecordBatchBuilder,
    RecordCapability, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, StreamReader,
    async_trait, read_object, scan_partition,
};
use std::sync::Arc;

/// One OCR page per row. Metadata fields are declared but remain null until a
/// separate metadata-join implementation is available. Shared scan layers own concurrency.
pub struct BhlPageAccessor<A> {
    storage: Arc<A>,
    snapshot_id: Arc<str>,
}
impl<A> BhlPageAccessor<A> {
    pub fn new(storage: A, snapshot_id: impl Into<String>) -> Result<Self> {
        let snapshot_id = snapshot_id.into();
        if snapshot_id.trim().is_empty() {
            return Err(super::invalid("BHL pages require a nonempty snapshot_id"));
        }
        Ok(Self {
            storage: Arc::new(storage),
            snapshot_id: snapshot_id.into(),
        })
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
        let identities = request
            .objects
            .iter()
            .map(|object| BhlPageIdentity::parse(object.locator()))
            .collect::<Result<Vec<_>>>()?;
        let full = page_schema();
        let output = request.options.resolve_projection(&full)?.schema;
        let metadata = RecordMetadata {
            schema: output.clone(),
            source: None,
        };
        let partitions = request
            .objects
            .into_iter()
            .zip(identities)
            .map(|(object, identity)| {
                let storage = self.storage.clone();
                let snapshot = self.snapshot_id.clone();
                let (full, output, metadata) = (full.clone(), output.clone(), metadata.clone());
                scan_partition(async move {
                    let batches = stream::once(async move {
                        let data = read_object(
                            storage.as_ref(),
                            ByteReadRequest::new(object.clone()).with_options(request.byte_options),
                            "read_bhl_page",
                        )
                        .await?;
                        RecordBatchBuilder::build_row(full, output, |row| {
                            row.append("entity_type", identity.entity_type.as_str())?;
                            row.append("entity_id", identity.entity_id)?;
                            row.append("page_id", identity.page_id)?;
                            row.append("sequence", identity.sequence)?;
                            row.append("data", data.as_slice())?;
                            row.append(
                                "source_path",
                                format!(
                                    "{}{}",
                                    super::super::OPEN_DATA_BUCKET_URL,
                                    object.locator()
                                )
                                .as_str(),
                            )?;
                            row.append("snapshot_id", snapshot.as_ref())
                            // Nullable metadata and source_entry_index are left unset (null).
                        })
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
