use super::{BhlMetadata, page_schema};
use crate::FileSetRequest;
use arrow_schema::Schema;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Capability, OrderRequirement, RecordBatchBuilder,
    RecordCapability, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, StreamReader,
    async_trait, read_object, scan_partition,
};
use std::{collections::HashMap, sync::Arc};

/// One lazy partition per selected OCR object, with full joined source metadata.
/// Generic scan layers own concurrency. Sidecars are preselected offline, so
/// planning never lists AWS or downloads the 13 whole-corpus metadata tables.
pub struct BhlPageAccessor<A> {
    storage: Arc<A>,
    metadata: Arc<BhlMetadata>,
}
impl<A> BhlPageAccessor<A> {
    pub fn new(storage: A, metadata: BhlMetadata) -> Self {
        Self {
            storage: Arc::new(storage),
            metadata: Arc::new(metadata),
        }
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
        let reports = request
            .objects
            .iter()
            .map(|object| {
                self.metadata
                    .report(object.locator())
                    .map(|r| (object.locator().to_owned(), r))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        let schema = page_schema();
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().clone(),
            HashMap::from([
                ("bhl.snapshot_id".into(), self.metadata.snapshot_id.clone()),
                (
                    "bhl.reconciliation".into(),
                    serde_json::to_string(&reports).expect("serializable reports"),
                ),
                (
                    "bhl.source_tables".into(),
                    serde_json::to_string(&self.metadata.source_tables)
                        .expect("serializable provenance"),
                ),
            ]),
        ));
        let output = request.options.resolve_projection(&schema)?.schema;
        let record_metadata = RecordMetadata {
            schema: output.clone(),
            source: None,
        };
        let partitions = request
            .objects
            .into_iter()
            .map(|object| {
                let storage = self.storage.clone();
                let metadata = self.metadata.clone();
                let full = schema.clone();
                let output = output.clone();
                let reader_metadata = record_metadata.clone();
                let byte_options = request.byte_options;
                scan_partition(async move {
                    let batches = stream::once(async move {
                        let joined = metadata.join(object.locator())?;
                        let data = read_object(
                            storage.as_ref(),
                            ByteReadRequest::new(object.clone()).with_options(byte_options),
                            "read_bhl_page",
                        )
                        .await?;
                        RecordBatchBuilder::build_row(full, output, |row| {
                            row.append("entity_type", joined.identity.entity_type.as_str())?;
                            row.append("entity_id", joined.identity.entity_id)?;
                            row.append("page_id", joined.identity.page_id)?;
                            row.append("sequence", joined.identity.sequence)?;
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
                            row.append("snapshot_id", metadata.snapshot_id.as_str())?;
                            joined.append(row)
                        })
                    });
                    Ok(StreamReader::records(
                        batches,
                        reader_metadata,
                        OrderRequirement::Reorderable,
                    ))
                })
            })
            .collect();
        Ok(ScanPlan::new(record_metadata, partitions).with_ordering(OrderRequirement::Reorderable))
    }
}
