use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::*;
use open_arrow_access_layers::{ParallelReadLayer, SamplingLayer};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

#[derive(Clone, Default)]
struct Source {
    opens: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
struct Input {
    batches: Vec<RecordBatch>,
    fail_after: Option<usize>,
}
impl Input {
    fn rows(n: usize, chunk: usize) -> Self {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ],
            [("test".to_owned(), "metadata".to_owned())].into(),
        ));
        let whole = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..n as i64)),
                Arc::new(StringArray::from_iter(
                    (0..n).map(|i| if i % 3 == 0 { None } else { Some("duplicate") }),
                )),
            ],
        )
        .unwrap();
        let mut batches = vec![whole.slice(0, 0)];
        for start in (0..n).step_by(chunk) {
            batches.push(whole.slice(start, chunk.min(n - start)));
        }
        Self {
            batches,
            fail_after: None,
        }
    }
}
struct DropGuard(Arc<AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn metadata(input: &Input) -> RecordMetadata {
    RecordMetadata {
        schema: input.batches[0].schema(),
        source: Some(ObjectRef::new("source")),
    }
}
#[async_trait]
impl Accessor for Source {
    type Request = Input;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }
    async fn open(&self, input: Input) -> Result<Box<dyn RecordReader>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        let metadata = metadata(&input);
        let mut batches = input.batches.into_iter();
        let polls = self.polls.clone();
        let guard = DropGuard(self.drops.clone());
        let mut seen = 0;
        let stream = stream::poll_fn(move |_| {
            let _guard = &guard;
            polls.fetch_add(1, Ordering::SeqCst);
            if input.fail_after == Some(seen) {
                return Poll::Ready(Some(Err(Error::new(
                    ErrorKind::Io,
                    "source_read",
                    "injected",
                )
                .with_source(std::io::Error::other("root cause")))));
            }
            seen += 1;
            Poll::Ready(batches.next().map(Ok))
        });
        Ok(Box::new(StreamReader::records(
            stream,
            metadata,
            OrderRequirement::PreserveGlobal,
        )))
    }
}
async fn ids(mut reader: Box<dyn RecordReader>) -> Vec<i64> {
    let mut ids = vec![];
    while let Some(batch) = reader.next().await.unwrap() {
        assert!(batch.num_rows() > 0);
        assert_eq!(batch.schema(), reader.metadata().schema);
        ids.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    ids
}
async fn sample(n: usize, chunk: usize, seed: u64) -> Vec<i64> {
    let accessor = Source::default().layer(SamplingLayer::try_new(0.5, seed).unwrap());
    ids(accessor.open(Input::rows(n, chunk)).await.unwrap()).await
}

#[test]
fn invalid_probabilities_are_configuration_errors() {
    for probability in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1, 1.1] {
        assert_eq!(
            SamplingLayer::try_new(probability, 0).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn seed_has_stable_selection_across_batches_and_opens() {
    let expected = vec![1, 2, 3, 4, 6, 8, 10, 11, 15, 16, 17, 18, 21];
    for chunk in [1, 3, 7, 24, 100] {
        assert_eq!(sample(24, chunk, 42).await, expected);
    }
    let source = Source::default().layer(SamplingLayer::try_new(0.5, 42).unwrap());
    assert_eq!(
        ids(source.open(Input::rows(24, 5)).await.unwrap()).await,
        expected
    );
    assert_eq!(
        ids(source.open(Input::rows(24, 5)).await.unwrap()).await,
        expected
    );
    assert_ne!(sample(1000, 13, 42).await, sample(1000, 13, 43).await);
}

#[tokio::test]
async fn rate_is_approximate_and_selected_rows_stay_in_order() {
    let sampled = sample(100_000, 1024, 7).await;
    assert!((49_000..51_000).contains(&sampled.len()));
    assert!(sampled.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn zero_releases_source_without_polling_and_one_passes_buffers_through() {
    let source = Source::default();
    let accessor = source
        .clone()
        .layer(SamplingLayer::try_new(0.0, 5).unwrap());
    let mut reader = accessor.open(Input::rows(10, 10)).await.unwrap();
    assert_eq!(source.opens.load(Ordering::SeqCst), 1);
    assert_eq!(source.drops.load(Ordering::SeqCst), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(source.polls.load(Ordering::SeqCst), 0);
    let mut input = Input::rows(10, 10);
    input.batches.remove(0);
    let array = input.batches[0].column(0).clone();
    let mut reader = Source::default()
        .layer(SamplingLayer::try_new(1.0, 0).unwrap())
        .open(input)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        reader.next().await.unwrap().unwrap().column(0),
        &array
    ));
}

#[tokio::test]
async fn preserves_metadata_null_values_and_zero_column_row_counts() {
    let input = Input::rows(100, 100);
    let metadata = metadata(&input);
    let mut reader = Source::default()
        .layer(SamplingLayer::try_new(0.5, 42).unwrap())
        .open(input)
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema, metadata.schema);
    assert_eq!(reader.metadata().source, metadata.source);
    assert_eq!(
        reader.capability().ordering,
        OrderRequirement::PreserveGlobal
    );
    let batch = reader.next().await.unwrap().unwrap();
    let expected = sample(100, 100, 42).await;
    for (index, id) in expected.iter().enumerate() {
        assert_eq!(batch.column(1).is_null(index), id % 3 == 0);
    }
    let mut input = Input::rows(100, 7);
    input.batches = input
        .batches
        .into_iter()
        .map(|batch| batch.project(&[]).unwrap())
        .collect();
    let mut reader = Source::default()
        .layer(SamplingLayer::try_new(0.5, 42).unwrap())
        .open(input)
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, expected.len());
}

#[tokio::test]
async fn errors_and_eof_release_upstream_and_are_terminal() {
    use std::error::Error as _;
    let source = Source::default();
    let mut input = Input::rows(100, 100);
    input.fail_after = Some(2);
    let mut reader = source
        .clone()
        .layer(SamplingLayer::try_new(0.5, 42).unwrap())
        .open(input)
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.source().unwrap().to_string(), "root cause");
    assert_eq!(source.drops.load(Ordering::SeqCst), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(source.polls.load(Ordering::SeqCst), 3);
    for n in [0, 1, 100] {
        let source = Source::default();
        let mut reader = source
            .clone()
            .layer(SamplingLayer::try_new(0.5, 42).unwrap())
            .open(Input::rows(n, 7))
            .await
            .unwrap();
        while reader.next().await.unwrap().is_some() {}
        assert_eq!(source.drops.load(Ordering::SeqCst), 1);
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn drop_stops_sampling_without_background_reads() {
    let source = Source::default();
    let mut reader = source
        .clone()
        .layer(SamplingLayer::try_new(0.5, 42).unwrap())
        .open(Input::rows(100, 10))
        .await
        .unwrap();
    assert_eq!(source.polls.load(Ordering::SeqCst), 0);
    assert!(reader.next().await.unwrap().is_some());
    let polls = source.polls.load(Ordering::SeqCst);
    drop(reader);
    tokio::task::yield_now().await;
    assert_eq!(source.polls.load(Ordering::SeqCst), polls);
    assert_eq!(source.drops.load(Ordering::SeqCst), 1);
}

#[async_trait]
impl ScanPlanner for Source {
    async fn plan_scan(&self, input: Input) -> Result<ScanPlan> {
        let metadata = metadata(&input);
        let source = self.clone();
        let parts = input
            .batches
            .into_iter()
            .map(|batch| {
                let source = source.clone();
                Box::pin(async move {
                    tokio::task::yield_now().await;
                    source
                        .open(Input {
                            batches: vec![batch],
                            fail_after: None,
                        })
                        .await
                }) as ScanPartition
            })
            .collect();
        Ok(ScanPlan::new(metadata, parts))
    }
}
#[tokio::test]
async fn ordered_parallel_execution_has_one_global_seed_stream() {
    for workers in [1, 2, 4] {
        let accessor = Source::default()
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(workers).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ))
            .layer(SamplingLayer::try_new(0.5, 42).unwrap());
        assert!(accessor.capability().read_parallel);
        let reader = accessor.open(Input::rows(100, 7)).await.unwrap();
        assert!(reader.capability().read_parallel);
        assert_eq!(ids(reader).await, sample(100, 100, 42).await);
    }
}

#[tokio::test]
async fn composes_with_csv_projection_and_caller_batch_settings() {
    use open_arrow_access_file_format::csv::{CsvAccessor, CsvFormat};
    use open_arrow_access_test_support::MemoryAccessor;
    let data = format!(
        "id,text\n{}",
        (0..24)
            .map(|id| format!("{id},duplicate\n"))
            .collect::<String>()
    );
    for batch_size in [1, 7, 100] {
        let schema = Input::rows(1, 1).batches[0].schema();
        let csv = CsvAccessor::new(MemoryAccessor::new(data.as_bytes(), 1), schema)
            .with_format(CsvFormat::default().with_header(true))
            .layer(SamplingLayer::try_new(0.5, 42).unwrap());
        let request = ReadRequest::new("input.csv").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(batch_size).unwrap(),
            projection: Some(vec!["id".into()]),
        });
        let reader = csv.open(request).await.unwrap();
        assert_eq!(reader.metadata().schema.fields().len(), 1);
        assert_eq!(ids(reader).await, sample(24, 24, 42).await);
    }
}
