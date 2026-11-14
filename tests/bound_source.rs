use std::sync::{Arc, Mutex};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, BoundSource, ErrorKind, OrderRequirement, Reader, RecordBatch, RecordCapability,
    RecordMetadata, RecordReader, Result, async_trait,
};

#[derive(Default)]
struct State {
    requests: Vec<u32>,
    schema_changed: bool,
}

struct TestAccessor(Arc<Mutex<State>>);

fn capability() -> RecordCapability {
    RecordCapability::sequential(OrderRequirement::PreserveGlobal)
}

#[async_trait]
impl Accessor for TestAccessor {
    // A provider-specific selector, without ReadRequest or ReadOptionsProvider.
    type Request = u32;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        capability()
    }

    async fn open(&self, request: u32) -> Result<Box<dyn RecordReader>> {
        let mut state = self.0.lock().unwrap();
        state.requests.push(request);
        let name = if state.schema_changed {
            "changed"
        } else {
            "id"
        };
        Ok(Box::new(TestReader(RecordMetadata {
            schema: Arc::new(Schema::new(vec![Field::new(name, DataType::UInt64, false)])),
            source: None,
        })))
    }
}

struct TestReader(RecordMetadata);

#[async_trait]
impl Reader for TestReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn capability(&self) -> RecordCapability {
        capability()
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.0
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        panic!("binding and obtaining a reader must not poll records")
    }
}

#[tokio::test]
async fn reuses_initial_reader_then_reopens_with_the_bound_request() {
    let state = Arc::new(Mutex::new(State::default()));
    let source = BoundSource::bind(TestAccessor(state.clone()), 42)
        .await
        .unwrap();
    assert_eq!(state.lock().unwrap().requests, [42]);
    let first = source.reader().await.unwrap();
    assert_eq!(state.lock().unwrap().requests, [42]);
    let second = source.reader().await.unwrap();
    assert_eq!(state.lock().unwrap().requests, [42, 42]);
    assert_eq!(first.metadata().schema, source.schema());
    assert_eq!(second.metadata().schema, source.schema());
}

#[tokio::test]
async fn rejects_schema_changes_on_reopen() {
    let state = Arc::new(Mutex::new(State::default()));
    let source = BoundSource::bind(TestAccessor(state.clone()), 42)
        .await
        .unwrap();
    drop(source.reader().await.unwrap());
    state.lock().unwrap().schema_changed = true;
    let error = source.reader().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert_eq!(error.operation(), "bound_source_reader");
    assert_eq!(source.schema().field(0).name(), "id");
}
