use super::super::{
    BhlPageRecord,
    metadata::{BhlMetadata, BhlPageMetadata},
};
use super::{BhlPageIdentity, page_schema};
use crate::FileSetRequest;
use arrow_array::{RecordBatch, StructArray};
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Capability, OrderRequirement,
    RecordCapability, RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner,
    SchemaRef, StreamReader, async_trait, read_object, scan_partition,
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
/// Projections without `data` skip OCR reads; empty projections keep one row.
pub struct BhlPageAccessor<A> {
    storage: Arc<A>,
    snapshot_id: Arc<str>,
    metadata: MetadataSource,
    schema_metadata: HashMap<String, String>,
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

    /// Execute one synchronous join per request on first read, buffer its
    /// metadata rows, then create independent OCR read partitions.
    /// `report` sees each page's missing/conflicting joins before its OCR read;
    /// return an error to stop the scan, or Ok(()) to retain the page.
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
        })
    }

    /// Merge caller provenance into the emitted batch schemas.
    pub fn with_schema_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.schema_metadata.extend(metadata);
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
        for object in &request.objects {
            BhlPageIdentity::parse(object.locator())?;
        }
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
        let partition = move |page| {
            page_partition(
                storage.clone(),
                snapshot.clone(),
                page,
                request.byte_options,
                partition_metadata.clone(),
            )
        };
        let plan = match &self.metadata {
            MetadataSource::Provider(provider) => {
                let pages = request
                    .objects
                    .into_iter()
                    .map(|object| {
                        let identity = BhlPageIdentity::parse(object.locator())?;
                        let metadata = provider(&identity)?;
                        Ok(BhlPageMetadata {
                            identity,
                            object,
                            metadata,
                            missing: vec![],
                            conflicts: vec![],
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                ScanPlan::from_iter(output, pages.into_iter().map(partition))
            }
            MetadataSource::DuckDb { metadata, report } => {
                let report = report.clone();
                let partitions = metadata.clone().stream(request.objects).map(move |page| {
                    let page = page?;
                    report(&page)?;
                    Ok(partition(page))
                });
                ScanPlan::from_stream(output, partitions).with_streaming_discovery()
            }
        };
        Ok(plan.with_ordering(OrderRequirement::Reorderable))
    }
}

fn page_partition<A: ByteAccessor + 'static>(
    storage: Arc<A>,
    snapshot: Arc<str>,
    page: BhlPageMetadata,
    byte_options: ByteReadOptions,
    metadata: RecordMetadata,
) -> ScanPartition {
    scan_partition(async move {
        let batches = stream::once(read_page_batch(
            storage,
            snapshot,
            page,
            byte_options,
            metadata.schema.clone(),
        ));
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
    page: BhlPageMetadata,
    byte_options: ByteReadOptions,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let data = if output_schema.field_with_name("data").is_ok() {
        read_object(
            storage.as_ref(),
            ByteReadRequest::new(page.object.clone()).with_options(byte_options),
            "read_bhl_page",
        )
        .await?
    } else {
        Default::default()
    };
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
}
