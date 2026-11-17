use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{
    Accessor, AccessorExt, OrderRequirement, ReadRequest, RecordCapability, RecordMetadata,
    RecordReader, Result, ScanPlan, ScanPlanner, StreamReader, async_trait, metrics::MetricsLayer,
};
use open_arrow_access_layers::{
    ColumnEvolution, LimitLayer, ParallelReadLayer, PrefetchLayer, SamplingLayer,
    SchemaEvolutionLayer, SourceColumns, UnorderedEmissionLayer,
};
use open_arrow_access_test_support::assert_metadata_matches_open;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Probe(Arc<AtomicUsize>);
#[async_trait]
impl Accessor for Probe {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::Reorderable)
    }
    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Ok(RecordMetadata {
            schema: request.options.resolve_projection(&schema)?.schema,
            source: Some(request.object),
        })
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let batches = futures::stream::once(async { panic!("inspection polled a batch") });
        Ok(Box::new(StreamReader::records(
            batches,
            self.metadata(request).await?,
            self.capability().ordering,
        )))
    }
}
#[async_trait]
impl ScanPlanner for Probe {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        Ok(ScanPlan::whole_input(self.open(request).await?))
    }
}

#[tokio::test]
async fn composed_layers_derive_matching_metadata_without_opening_or_planning() {
    let opens = Arc::new(AtomicUsize::new(0));
    let source = Probe(opens.clone())
        .layer(ParallelReadLayer::new(NonZeroUsize::MIN, NonZeroUsize::MIN))
        .layer(UnorderedEmissionLayer::new())
        .layer(MetricsLayer::default())
        .layer(PrefetchLayer::rows(NonZeroUsize::MIN))
        .layer(LimitLayer::new(2))
        .layer(SamplingLayer::try_new(0.5, 42).unwrap());
    let source = SourceColumns::new().with_object("path").wrap(source).layer(
        SchemaEvolutionLayer::try_new(vec![ColumnEvolution::change_data_type(
            "id",
            DataType::Utf8,
        )])
        .unwrap(),
    );
    let mut request = ReadRequest::new("object");
    request.options.projection = Some(vec!["path".into(), "id".into()]);
    let metadata = source.metadata(request.clone()).await.unwrap();
    assert_eq!(metadata.schema.field(0).name(), "path");
    assert_eq!(metadata.schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    assert_metadata_matches_open(&source, request).await;
}
