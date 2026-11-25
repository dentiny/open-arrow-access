use super::super::{
    BhlPageRecord,
    metadata::{BhlMetadata, BhlPageMetadata, BhlPageSelection},
};
use super::{
    BhlPageIdentity, BhlPageReconciliation, page_schema,
    reconciliation::{self, ReconciliationReport},
};
use crate::FileSetRequest;
use arrow_array::{RecordBatch, StructArray};
use futures::{StreamExt, TryStreamExt, future, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, Capability, OrderRequirement, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, SchemaRef,
    StreamReader, async_trait, scan_partition,
};
use std::{collections::HashMap, sync::Arc};

type MetadataProvider = dyn Fn(&BhlPageIdentity) -> Result<StructArray> + Send + Sync;
type MetadataReport = dyn Fn(&BhlPageMetadata) -> Result<()> + Send + Sync;

enum MetadataSource {
    Provider(Arc<MetadataProvider>),
    DuckDb {
        metadata: Arc<BhlMetadata>,
        report: Arc<MetadataReport>,
    },
}

/// Read selected OCR pages with caller-supplied or DuckDB-joined metadata.
/// Unique pages projected without `data` skip OCR reads. Canonical aliases are
/// read even for metadata-only projections to verify equality, allowing LF/CRLF.
pub struct BhlPageAccessor<A> {
    storage: Arc<A>,
    snapshot_id: Arc<str>,
    metadata: MetadataSource,
    schema_metadata: HashMap<String, String>,
    reconciliation_report: ReconciliationReport,
}
impl<A> BhlPageAccessor<A> {
    pub fn new(
        storage: A,
        snapshot_id: impl Into<String>,
        metadata: impl Fn(&BhlPageIdentity) -> Result<StructArray> + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::build(
            storage,
            snapshot_id.into(),
            MetadataSource::Provider(Arc::new(metadata)),
        )
    }

    /// Execute one join per request on a blocking worker on first read, then
    /// stream metadata through a bounded queue into independent OCR partitions.
    /// `report` validates each surviving canonical page before its OCR read;
    /// return an error to stop the scan, or Ok(()) to retain the page.
    /// Objects without a metadata-authoritative sequence, and canonical groups
    /// with contradictory parent metadata, are audited and quarantined before
    /// this callback. Missing required joins still reach the callback.
    /// Canonical aliases are emitted once after byte verification.
    pub fn from_metadata(
        storage: A,
        snapshot_id: impl Into<String>,
        metadata: Arc<BhlMetadata>,
        report: impl Fn(&BhlPageMetadata) -> Result<()> + Send + Sync + 'static,
    ) -> Result<Self> {
        let provenance = metadata.schema_metadata()?;
        Ok(Self::build(
            storage,
            snapshot_id.into(),
            MetadataSource::DuckDb {
                metadata,
                report: Arc::new(report),
            },
        )?
        .with_schema_metadata(provenance))
    }

    fn build(storage: A, snapshot_id: String, metadata: MetadataSource) -> Result<Self> {
        if snapshot_id.trim().is_empty() {
            return Err(super::invalid("BHL pages require a nonempty snapshot_id"));
        }
        Ok(Self {
            storage: Arc::new(storage),
            snapshot_id: snapshot_id.into(),
            metadata,
            schema_metadata: HashMap::new(),
            reconciliation_report: Arc::new(reconciliation::log_reconciliation),
        })
    }

    /// Merge caller provenance into the emitted batch schemas.
    pub fn with_schema_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.schema_metadata.extend(metadata);
        self
    }

    /// Replace the default stderr audit with a structured reconciliation sink.
    /// An error from the sink stops the scan; quarantines are never unreported.
    pub fn with_reconciliation_report(
        mut self,
        report: impl Fn(&BhlPageReconciliation) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.reconciliation_report = Arc::new(report);
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
        let identities = request
            .objects
            .iter()
            .map(|object| BhlPageIdentity::parse(object.locator()))
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(
            page_schema()
                .as_ref()
                .clone()
                .with_metadata(self.schema_metadata.clone()),
        );
        let projection = request.options.resolve_projection(&schema)?;
        let output = RecordMetadata {
            schema: projection.schema,
            source: None,
        };
        let storage = self.storage.clone();
        let snapshot = self.snapshot_id.clone();
        let partition_metadata = output.clone();
        let reconciliation_report = self.reconciliation_report.clone();
        let completion = Arc::new(reconciliation::ScanCompletion::default());
        let partition_completion = completion.clone();
        let partition = move |page: BhlPageSelection| {
            partition_completion.selected(&page.page.object);
            page_partition(
                storage.clone(),
                snapshot.clone(),
                page,
                request.byte_options,
                partition_metadata.clone(),
                reconciliation_report.clone(),
                partition_completion.clone(),
            )
        };
        let mut plan = match &self.metadata {
            MetadataSource::Provider(provider) => {
                let pages = request
                    .objects
                    .into_iter()
                    .zip(identities)
                    .map(|(object, identity)| {
                        let metadata = provider(&identity)?;
                        Ok(BhlPageSelection {
                            objects: vec![object.clone()],
                            page: BhlPageMetadata {
                                identity,
                                object,
                                metadata,
                                missing: vec![],
                                conflicts: vec![],
                            },
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                ScanPlan::from_iter(output, pages.into_iter().map(partition))
            }
            MetadataSource::DuckDb { metadata, report } => {
                let report = report.clone();
                let reconciliation_report = self.reconciliation_report.clone();
                let partitions =
                    metadata
                        .clone()
                        .stream(request.objects)
                        .try_filter_map(move |selection| {
                            let result = if selection.objects.is_empty() {
                                let page = selection.page;
                                reconciliation_report(&BhlPageReconciliation::Quarantined {
                                    object: page.object,
                                    reason: "no OCR key matches authoritative SequenceOrder",
                                    missing: page.missing,
                                    conflicts: page.conflicts,
                                })
                                .map(|()| None)
                            } else if !selection.page.conflicts.is_empty() {
                                let page = selection.page;
                                reconciliation_report(&BhlPageReconciliation::ConflictingMetadata {
                                    canonical: page.object,
                                    aliases: selection.objects,
                                    missing: page.missing,
                                    conflicts: page.conflicts,
                                })
                                .map(|()| None)
                            } else {
                                report(&selection.page).map(|()| Some(partition(selection)))
                            };
                            future::ready(result)
                        });
                ScanPlan::from_stream(output, partitions).with_streaming_discovery()
            }
        };
        // Discovery may finish before parallel OCR workers, or after the last
        // sequential partition. Both paths enforce the same nonempty-output rule.
        plan.partitions = plan
            .partitions
            .chain(
                stream::once(async move {
                    completion.discovery_finished()?;
                    Ok(None)
                })
                .try_filter_map(future::ready),
            )
            .boxed();
        Ok(plan.with_ordering(OrderRequirement::Reorderable))
    }
}

fn page_partition<A: ByteAccessor + 'static>(
    storage: Arc<A>,
    snapshot: Arc<str>,
    selection: BhlPageSelection,
    byte_options: ByteReadOptions,
    metadata: RecordMetadata,
    report: ReconciliationReport,
    completion: Arc<reconciliation::ScanCompletion>,
) -> ScanPartition {
    scan_partition(async move {
        let batches = stream::once(read_page_batch(
            storage,
            snapshot,
            selection,
            byte_options,
            metadata.schema.clone(),
            report,
        ))
        .try_filter_map(move |batch| {
            let result = completion.completed(batch.is_some()).map(|()| batch);
            future::ready(result)
        });
        Ok(StreamReader::records(
            batches,
            metadata,
            OrderRequirement::Reorderable,
        ))
    })
}

async fn read_page_batch<A: ByteAccessor>(
    storage: Arc<A>,
    snapshot: Arc<str>,
    selection: BhlPageSelection,
    byte_options: ByteReadOptions,
    output_schema: SchemaRef,
    report: ReconciliationReport,
) -> Result<Option<RecordBatch>> {
    let data = if output_schema.field_with_name("data").is_ok() || selection.objects.len() > 1 {
        let Some(data) =
            reconciliation::read_canonical(storage.as_ref(), &selection, byte_options, &report)
                .await?
        else {
            return Ok(None);
        };
        data
    } else {
        Default::default()
    };
    let page = selection.page;
    let source_path = format!(
        "{}{}",
        super::super::OPEN_DATA_BUCKET_URL,
        page.object.locator()
    );
    BhlPageRecord {
        entity_type: &page.identity.entity_type,
        entity_id: page.identity.entity_id,
        page_id: page.identity.page_id,
        sequence: page.identity.sequence,
        data: &data,
        source_path: &source_path,
        snapshot_id: &snapshot,
        source_entry_index: None,
        metadata: page.metadata,
    }
    .into_projected_batch(output_schema)
    .map(Some)
}
