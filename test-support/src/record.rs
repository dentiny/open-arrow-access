//! In-memory record sources and helpers for comparing reader output.

use arrow_array::{Array, cast::AsArray, types::ArrowPrimitiveType};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use futures::{TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, OrderRequirement, ReadRequest, RecordBatch, RecordCapability, RecordMetadata,
    RecordReader, Result, SchemaRef, async_trait, into_stream, record_stream_reader,
};

/// Collect a non-null primitive column in batch order.
pub fn collect_values<T: ArrowPrimitiveType>(
    batches: &[RecordBatch],
    column: usize,
) -> Vec<T::Native> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch.column(column).as_primitive::<T>();
        assert_eq!(
            array.null_count(),
            0,
            "collect_values requires non-null values"
        );
        values.extend_from_slice(array.values());
    }
    values
}

/// Read all batches and collect a non-null primitive column.
pub async fn collect_column<T: ArrowPrimitiveType>(
    reader: Box<dyn RecordReader>,
    column: usize,
) -> Vec<T::Native> {
    let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
    collect_values::<T>(&batches, column)
}

/// An in-memory record source with independent cursors for each open.
/// Projections and observations are shared across clones and opened readers.
#[derive(Clone)]
pub struct MemoryRecordAccessor {
    pub schema: SchemaRef,
    batches: Vec<RecordBatch>,
    /// Input stream polls, including the first EOF poll (but not reads after EOF).
    pub polls: Arc<AtomicUsize>,
    /// Input batches yielded, including batches with zero rows; excludes EOF.
    pub batches_read: Arc<AtomicUsize>,
    /// Requested projections in open order, including requests that fail validation.
    pub projections: Arc<Mutex<Vec<Option<Vec<String>>>>>,
}

impl MemoryRecordAccessor {
    /// Derive the fixed source schema from the first batch. Requires at least one batch.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            schema: batches
                .first()
                .expect("record fixture requires at least one batch")
                .schema(),
            batches,
            polls: Arc::default(),
            batches_read: Arc::default(),
            projections: Arc::default(),
        }
    }
}

#[async_trait]
impl Accessor for MemoryRecordAccessor {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::Reorderable)
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.projections
            .lock()
            .unwrap()
            .push(request.options.projection.clone());
        let (schema, projection) = request.options.resolve_projection(&self.schema)?;
        let mut batches = self.batches.clone().into_iter();
        let polls = self.polls.clone();
        let batches_read = self.batches_read.clone();
        let stream = stream::poll_fn(move |_| {
            polls.fetch_add(1, Ordering::SeqCst);
            let batch = batches.next();
            if batch.is_some() {
                batches_read.fetch_add(1, Ordering::SeqCst);
            }
            Poll::Ready(batch.map(Ok))
        });
        Ok(Box::new(record_stream_reader(
            stream,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            projection,
            OrderRequirement::Reorderable,
        )))
    }
}
