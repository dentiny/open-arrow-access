use std::{num::NonZeroUsize, sync::Mutex};

use arrow_array::record_batch;
use futures::stream;
use open_arrow_access_core::{
    Accessor, OrderRequirement, ReadOptions, ReadOptionsProvider, ReadRequest, RecordCapability,
    RecordMetadata, RecordReader, Result, SelectionAccessor, StreamReader, async_trait,
    project_batch,
};

#[derive(Clone)]
struct PageRequest {
    cursor: String,
    read: ReadRequest,
}

impl ReadOptionsProvider for PageRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.read.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.read.options
    }
}

#[derive(Default)]
struct Pages {
    calls: Mutex<Vec<(&'static str, PageRequest)>>,
    empty: bool,
}

fn metadata(request: &PageRequest) -> RecordMetadata {
    let schema = record_batch!(("id", Int32, [1, 2]), ("label", Utf8, ["a", "b"]))
        .unwrap()
        .schema();
    RecordMetadata {
        schema: request
            .read
            .options
            .resolve_projection(&schema)
            .unwrap()
            .schema,
        source: Some(request.read.object.clone()),
    }
}

#[async_trait]
impl Accessor for Pages {
    type Request = PageRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn metadata(&self, request: PageRequest) -> Result<RecordMetadata> {
        let metadata = metadata(&request);
        self.calls.lock().unwrap().push(("metadata", request));
        Ok(metadata)
    }

    async fn inspect_metadata(&self, request: PageRequest) -> Result<Option<RecordMetadata>> {
        let metadata = (!self.empty).then(|| metadata(&request));
        self.calls.lock().unwrap().push(("inspect", request));
        Ok(metadata)
    }

    async fn open(&self, request: PageRequest) -> Result<Box<dyn RecordReader>> {
        let batch = record_batch!(("id", Int32, [1, 2]), ("label", Utf8, ["a", "b"])).unwrap();
        let projection = request.read.options.resolve_projection(&batch.schema())?;
        let batch = project_batch(batch, projection.indices.as_deref(), projection.schema)?;
        let metadata = metadata(&request);
        self.calls.lock().unwrap().push(("open", request));
        Ok(Box::new(StreamReader::records(
            stream::iter([Ok(batch)]),
            metadata,
            OrderRequirement::PreserveGlobal,
        )))
    }
}

fn bound() -> PageRequest {
    let mut read = ReadRequest::new("selected.json")
        .with_batch_size(NonZeroUsize::new(17).unwrap())
        .with_projection(["id"]);
    read.byte_options.io_concurrency = NonZeroUsize::new(2).unwrap();
    PageRequest {
        cursor: "page-2".into(),
        read,
    }
}

fn incoming() -> ReadRequest {
    let mut request = ReadRequest::new("ignored.json")
        .with_batch_size(NonZeroUsize::new(3).unwrap())
        .with_projection(["label"]);
    request.byte_options.io_concurrency = NonZeroUsize::new(9).unwrap();
    request
}

fn assert_selected(request: &PageRequest, projected: bool) {
    assert_eq!(request.cursor, "page-2");
    assert_eq!(request.read.object.locator(), "selected.json");
    assert_eq!(request.read.byte_options.io_concurrency.get(), 2);
    if projected {
        assert_eq!(request.read.options.batch_size.get(), 3);
        assert_eq!(
            request.read.options.projection.as_deref(),
            Some(["label".to_owned()].as_slice())
        );
    } else {
        assert_eq!(
            request.read.options.batch_size,
            ReadOptions::default().batch_size
        );
        assert!(request.read.options.projection.is_none());
    }
}

#[tokio::test]
async fn fixes_selection_and_replaces_options_for_independent_reads() {
    // A shared trait object also works; neither Pages nor the adapter needs Clone.
    let pages = std::sync::Arc::new(Pages::default());
    let inner: std::sync::Arc<dyn Accessor<Request = PageRequest, Reader = dyn RecordReader>> =
        pages.clone();
    let selected = SelectionAccessor::new(inner, bound());
    assert!(pages.calls.lock().unwrap().is_empty());
    assert_eq!(selected.capability(), pages.capability());
    let mut first = selected.open(incoming()).await.unwrap();
    let mut second = selected
        .open(ReadRequest::new("also-ignored"))
        .await
        .unwrap();
    assert_eq!(first.metadata().schema.fields().len(), 1);
    assert_eq!(first.metadata().schema.field(0).name(), "label");
    assert_eq!(second.metadata().schema.fields().len(), 2);
    assert_eq!(second.next().await.unwrap().unwrap().num_rows(), 2);
    assert_eq!(first.next().await.unwrap().unwrap().num_columns(), 1);
    assert!(first.next().await.unwrap().is_none());
    assert!(second.next().await.unwrap().is_none());
    let calls = pages.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_selected(&calls[0].1, true);
    assert_selected(&calls[1].1, false);
}

#[tokio::test]
async fn metadata_and_inspection_delegate_without_opening_readers() {
    let pages = std::sync::Arc::new(Pages::default());
    let selected = SelectionAccessor::new(pages.clone(), bound());
    let direct = selected.metadata(incoming()).await.unwrap();
    let inspected = selected
        .inspect_metadata(incoming())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(direct, inspected);
    let mut reader = selected.open(incoming()).await.unwrap();
    assert_eq!(&direct, reader.metadata());
    assert_eq!(reader.next().await.unwrap().unwrap().num_columns(), 1);
    let calls = pages.calls.lock().unwrap();
    assert_eq!(
        calls.iter().map(|(method, _)| *method).collect::<Vec<_>>(),
        ["metadata", "inspect", "open"]
    );
    for (_, request) in calls.iter() {
        assert_selected(request, true);
    }
}

#[tokio::test]
async fn preserves_proven_empty_inspection() {
    let pages = std::sync::Arc::new(Pages {
        empty: true,
        ..Pages::default()
    });
    let selected = SelectionAccessor::new(pages.clone(), bound());
    assert!(
        selected
            .inspect_metadata(incoming())
            .await
            .unwrap()
            .is_none()
    );
    let calls = pages.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "inspect");
    assert_selected(&calls[0].1, true);
}
