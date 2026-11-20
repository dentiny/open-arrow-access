use super::*;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{AccessorExt, OrderRequirement, RecordMetadata, SchemaRef};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
enum Event {
    Batch(Vec<i32>),
    Error(ErrorKind),
}

struct Source {
    open_error: Option<ErrorKind>,
    events: Vec<Event>,
    schema: SchemaRef,
    requests: Arc<Mutex<Vec<u32>>>,
    opens: Arc<AtomicUsize>,
    ordering: OrderRequirement,
}

impl Source {
    fn new(events: Vec<Event>) -> Self {
        Self {
            open_error: None,
            events,
            schema: Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
            requests: Arc::default(),
            opens: Arc::default(),
            ordering: OrderRequirement::Reorderable,
        }
    }

    fn absent() -> Self {
        let mut source = Self::new(vec![]);
        source.open_error = Some(ErrorKind::NotFound);
        source
    }
}

fn failure(kind: ErrorKind) -> Error {
    Error::new(kind, "source_read", "source failure")
}

#[async_trait]
impl Accessor for Source {
    type Request = u32;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(self.ordering)
    }

    async fn open(&self, request: u32) -> Result<Box<dyn RecordReader>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        if let Some(kind) = self.open_error {
            return Err(failure(kind));
        }
        let schema = self.schema.clone();
        let events = self
            .events
            .clone()
            .into_iter()
            .map(move |event| match event {
                Event::Batch(values) => Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(values))],
                )
                .unwrap()),
                Event::Error(kind) => Err(failure(kind)),
            });
        Ok(Box::new(StreamReader::records(
            stream::iter(events),
            RecordMetadata {
                schema: self.schema.clone(),
                source: None,
            },
            self.ordering,
        )))
    }
}

async fn values(mut reader: Box<dyn RecordReader>) -> Result<Vec<i32>> {
    use arrow_array::cast::AsArray;
    let mut values = Vec::new();
    while let Some(batch) = reader.next().await? {
        values.extend(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values(),
        );
    }
    Ok(values)
}

#[tokio::test]
async fn primary_success_is_delivered_once_without_opening_fallback() {
    let fallback = Source::new(vec![Event::Batch(vec![99])]);
    let opens = fallback.opens.clone();
    let accessor = Source::new(vec![Event::Batch(vec![1, 2]), Event::Batch(vec![3])])
        .layer(FallbackLayer::new(fallback));
    assert_eq!(
        values(accessor.open(7).await.unwrap()).await.unwrap(),
        [1, 2, 3]
    );
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[rstest::rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn missing_open_or_first_batch_selects_fallback_and_its_metadata(#[case] at_open: bool) {
    let primary = if at_open {
        Source::absent()
    } else {
        Source::new(vec![Event::Error(ErrorKind::NotFound)])
    };
    let mut fallback = Source::new(vec![Event::Batch(vec![4, 5])]);
    fallback.schema = Arc::new(Schema::new(vec![Field::new(
        "alternative",
        DataType::Int32,
        false,
    )]));
    let schema = fallback.schema.clone();
    let primary_requests = primary.requests.clone();
    let fallback_requests = fallback.requests.clone();
    let accessor = primary.layer(FallbackLayer::new(fallback));
    assert_eq!(accessor.metadata(42).await.unwrap().schema, schema);
    let reader = accessor.open(42).await.unwrap();
    assert_eq!(reader.metadata().schema, schema);
    assert_eq!(values(reader).await.unwrap(), [4, 5]);
    assert_eq!(*primary_requests.lock().unwrap(), [42, 42]);
    assert_eq!(*fallback_requests.lock().unwrap(), [42, 42]);
}

#[rstest::rstest]
#[case(ErrorKind::PermissionDenied)]
#[case(ErrorKind::Unauthenticated)]
#[case(ErrorKind::Timeout)]
#[case(ErrorKind::RateLimited)]
#[case(ErrorKind::Decode)]
#[case(ErrorKind::Io)]
#[tokio::test]
async fn default_policy_preserves_fatal_errors(#[case] kind: ErrorKind) {
    for at_open in [true, false] {
        let mut primary = Source::new(vec![Event::Error(kind)]);
        primary.open_error = at_open.then_some(kind);
        let fallback = Source::new(vec![Event::Batch(vec![9])]);
        let opens = fallback.opens.clone();
        let accessor = primary.layer(FallbackLayer::new(fallback));
        let error = accessor.open(0).await.err().unwrap();
        assert_eq!(error.kind(), kind);
        assert_eq!(error.operation(), "source_read");
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }
}

#[rstest::rstest]
#[case(vec![1])]
#[case(vec![])]
#[tokio::test]
async fn failures_after_any_selected_batch_never_switch(#[case] first: Vec<i32>) {
    let primary = Source::new(vec![
        Event::Batch(first.clone()),
        Event::Error(ErrorKind::NotFound),
    ]);
    let fallback = Source::new(vec![Event::Batch(vec![9])]);
    let opens = fallback.opens.clone();
    let accessor = primary.layer(FallbackLayer::new(fallback));
    let mut reader = accessor.open(0).await.unwrap();
    assert_eq!(
        reader.next().await.unwrap().unwrap().num_rows(),
        first.len()
    );
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn successful_empty_stream_is_not_missing() {
    let fallback = Source::new(vec![Event::Batch(vec![9])]);
    let opens = fallback.opens.clone();
    let accessor = Source::new(vec![]).layer(FallbackLayer::new(fallback));
    assert!(
        values(accessor.open(0).await.unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn explicit_condition_and_chained_candidates_preserve_final_error() {
    let secondary =
        Source::absent().layer(FallbackLayer::new(Source::new(vec![Event::Batch(vec![8])])));
    let primary = Source::new(vec![Event::Error(ErrorKind::Unsupported)]);
    let accessor = primary
        .layer(FallbackLayer::new(secondary).when(|error| error.kind() == ErrorKind::Unsupported));
    assert_eq!(values(accessor.open(0).await.unwrap()).await.unwrap(), [8]);
    let primary = Source::absent();
    let fallback = Source::new(vec![Event::Error(ErrorKind::PermissionDenied)]);
    let error = primary
        .layer(FallbackLayer::new(fallback))
        .open(0)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "source_read");
}

#[test]
fn capability_retains_stricter_ordering_without_exposing_discovery() {
    let primary = Source::new(vec![]);
    let mut fallback = Source::new(vec![]);
    fallback.ordering = OrderRequirement::PreserveGlobal;
    let layer = FallbackLayer::new(fallback);
    let accessor = primary.layer(layer.clone());
    assert_eq!(
        accessor.capability(),
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    );
}
