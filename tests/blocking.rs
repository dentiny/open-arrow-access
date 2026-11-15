#![cfg(feature = "blocking")]

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow_array::{Int32Array, RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use open_arrow_access::{
    BlockingRecordBatchReader, Error, ErrorKind, OrderRequirement, Reader, RecordCapability,
    RecordMetadata, Result, async_trait,
};
use tokio::runtime::{Handle, Runtime};

enum End {
    Eof,
    Error,
    Panic,
}

struct TestReader {
    metadata: RecordMetadata,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    batches: usize,
    end: End,
}

#[async_trait]
impl Reader for TestReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        // Exercise the supplied runtime's timer driver, not only ready futures.
        tokio::time::sleep(Duration::from_millis(1)).await;
        if index < self.batches {
            return Ok(Some(
                RecordBatch::try_new(
                    self.metadata.schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![index as i32]))],
                )
                .unwrap(),
            ));
        }
        assert_eq!(index, self.batches, "terminal reader polled again");
        match self.end {
            End::Eof => Ok(None),
            End::Error => Err(Error::new(
                ErrorKind::PermissionDenied,
                "test_read",
                "denied",
            )),
            End::Panic => panic!("injected reader panic"),
        }
    }
}

impl Drop for TestReader {
    fn drop(&mut self) {
        assert!(Handle::try_current().is_ok(), "drop needs runtime context");
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn fixture(batches: usize, end: End) -> (TestReader, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int32, false)],
        HashMap::from([("provider".into(), "test".into())]),
    ));
    (
        TestReader {
            metadata: RecordMetadata {
                schema,
                source: None,
            },
            calls: calls.clone(),
            drops: drops.clone(),
            batches,
            end,
        },
        calls,
        drops,
    )
}

#[test]
fn schema_is_available_before_polling_and_eof_is_terminal() {
    let runtime = Arc::new(Runtime::new().unwrap());
    let (input, calls, drops) = fixture(2, End::Eof);
    let schema = input.metadata.schema.clone();
    let mut reader = BlockingRecordBatchReader::new(Box::new(input), runtime);
    assert_eq!(reader.schema(), schema);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for expected in 0..2 {
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema(), schema);
        assert_eq!(
            batch["id"]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            expected,
        );
    }
    assert!(reader.next().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(reader.next().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(reader.schema(), schema);
}

#[test]
fn upstream_error_preserves_category_and_operation_and_ends_consumption() {
    let (input, calls, drops) = fixture(0, End::Error);
    let mut reader =
        BlockingRecordBatchReader::new(Box::new(input), Arc::new(Runtime::new().unwrap()));
    let ArrowError::ExternalError(error) = reader.next().unwrap().unwrap_err() else {
        panic!("upstream error category erased");
    };
    let error = error.downcast_ref::<Error>().unwrap();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "test_read");
    assert!(reader.next().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn panicking_reader_returns_one_terminal_arrow_error() {
    let (input, calls, drops) = fixture(0, End::Panic);
    let mut reader =
        BlockingRecordBatchReader::new(Box::new(input), Arc::new(Runtime::new().unwrap()));
    let error = reader.next().unwrap().unwrap_err();
    assert!(error.to_string().contains("source reader panicked"));
    assert!(reader.next().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn nested_runtime_returns_terminal_error_without_polling() {
    let runtime = Arc::new(Runtime::new().unwrap());
    let (input, calls, drops) = fixture(1, End::Eof);
    let mut reader = BlockingRecordBatchReader::new(Box::new(input), runtime.clone());
    runtime.block_on(async {
        let error = reader.next().unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("outside a Tokio runtime context")
        );
        assert!(reader.next().is_none());
    });
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn reader_keeps_runtime_alive_and_can_move_to_a_synchronous_consumer() {
    let runtime = Arc::new(Runtime::new().unwrap());
    let weak = Arc::downgrade(&runtime);
    let (input, calls, drops) = fixture(2, End::Eof);
    let reader = BlockingRecordBatchReader::new(Box::new(input), runtime);
    assert!(weak.upgrade().is_some());
    std::thread::spawn(move || {
        let mut reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        assert!(reader.next().unwrap().is_ok());
        // Partial consumption drops the live reader in the runtime context.
        drop(reader);
    })
    .join()
    .unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
