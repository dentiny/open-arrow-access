use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{RecordBatchOptions, cast::AsArray};
use arrow_schema::{DataType, Field};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, Capability, DefaultDocumentAccessor,
    DocumentAccessor, DocumentDecoder, Error, ErrorKind, ReadOptions, ReadRequest, RecordBatch,
    RecordBatchBuilder, Result, Schema, SchemaRef, async_trait,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture};

#[derive(Default)]
struct RawDocument;

#[async_trait]
impl DocumentDecoder for RawDocument {
    type Request = ReadRequest;

    fn prepare(&self, _: &ReadRequest) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::LargeBinary,
            false,
        )])))
    }

    async fn decode(
        &self,
        bytes: &[u8],
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let mut builder =
            RecordBatchBuilder::new(self.prepare(&request)?, request.options.projection)?;
        builder.push(|row| row.append("data", bytes))?;
        let batch = builder.finish()?;
        assert_eq!(batch.schema(), schema);
        Ok(batch)
    }
}

// Neither the storage nor the decoder implements Clone. Accessor clones share
// their ownership handles while each opened reader keeps independent state.
struct NonCloneStorage(MemoryAccessor);

#[async_trait]
impl Accessor for NonCloneStorage {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        self.0.capability()
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.0.open(request).await
    }
}

#[tokio::test]
async fn default_decoder_entrypoint_clones_non_clone_inputs_and_opens_independent_readers() {
    let source = MemoryAccessor::new(&fixture!("bytes", "binary.bin").await, 1);
    let observed = source.observations.clone();
    let accessor = DefaultDocumentAccessor::<_, RawDocument>::new(NonCloneStorage(source));
    let cloned = accessor.clone();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        ..Default::default()
    };
    let mut projected = accessor
        .open(ReadRequest::new("projected").with_options(ReadOptions {
            projection: Some(vec![]),
            ..Default::default()
        }))
        .await
        .unwrap();
    let mut full = cloned
        .open(ReadRequest::new("full").with_byte_options(options))
        .await
        .unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    assert!(accessor.capability().read);
    assert!(!accessor.capability().read_with_range);
    assert!(!accessor.capability().list);
    drop(accessor);
    drop(cloned);
    let full_batch = full.next().await.unwrap().unwrap();
    assert_eq!(
        full_batch["data"].as_binary::<i64>().value(0),
        &[0, 255, 128]
    );
    let empty_batch = projected.next().await.unwrap().unwrap();
    assert_eq!(empty_batch.num_rows(), 1);
    assert_eq!(empty_batch.num_columns(), 0);
    assert!(full.next().await.unwrap().is_none());
    assert!(projected.next().await.unwrap().is_none());
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].object.locator(), "full");
    assert_eq!(requests[0].options, options);
    assert_eq!(requests[1].object.locator(), "projected");
}

#[tokio::test]
async fn default_decoder_entrypoint_validates_projection_before_storage_io() {
    let source = MemoryAccessor::new(&fixture!("bytes", "input.bin").await, 1);
    let observed = source.observations.clone();
    let accessor = DefaultDocumentAccessor::<_, RawDocument>::new(source);
    let request = ReadRequest::new("input").with_options(ReadOptions {
        projection: Some(vec!["unknown".into()]),
        ..ReadOptions::default()
    });
    let error = accessor.open(request).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reads_once_lazily_with_byte_options_projection_and_owned_state() {
    for projection in [None, Some(vec![])] {
        let source = MemoryAccessor::new(&fixture!("bytes", "binary.bin").await, 1);
        let observed = source.observations.clone();
        let accessor = DocumentAccessor::new(source, RawDocument);
        let options = ByteReadOptions {
            io_concurrency: NonZeroUsize::new(2).unwrap(),
            ..Default::default()
        };
        let mut reader = accessor
            .open(
                ReadRequest::new("book.rdf")
                    .with_options(ReadOptions {
                        projection,
                        ..Default::default()
                    })
                    .with_byte_options(options),
            )
            .await
            .unwrap();
        assert!(observed.requests.lock().unwrap().is_empty());
        let schema = reader.metadata().schema.clone();
        drop(accessor);
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(Arc::ptr_eq(&batch.schema(), &schema));
        if batch.num_columns() != 0 {
            assert_eq!(batch.column(0).as_binary::<i64>().value(0), &[0, 255, 128]);
        }
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].options, options);
        assert_eq!(requests[0].object.locator(), "book.rdf");
    }
}

#[tokio::test]
async fn rejects_bad_projection_and_preserves_source_errors() {
    let source = MemoryAccessor::new(&fixture!("bytes", "input.bin").await, 1);
    let observed = source.observations.clone();
    let accessor = DocumentAccessor::new(source, RawDocument);
    let request = ReadRequest::new("input").with_options(ReadOptions {
        projection: Some(vec!["unknown".into()]),
        ..Default::default()
    });
    assert_eq!(
        accessor.open(request).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(observed.requests.lock().unwrap().is_empty());
    let mut reader = accessor.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());

    let source = MemoryAccessor::new(&fixture!("bytes", "input.bin").await, 1)
        .with_failure(0, ErrorKind::PermissionDenied);
    let mut reader = DocumentAccessor::new(source, RawDocument)
        .open(ReadRequest::new("input"))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "memory_read");
    assert!(reader.next().await.unwrap().is_none());
}

enum InvalidDocument {
    Empty,
    MultipleRows,
    WrongSchema,
    DecodeError,
}

#[async_trait]
impl DocumentDecoder for InvalidDocument {
    type Request = ReadRequest;
    fn prepare(&self, _: &ReadRequest) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::empty()))
    }
    async fn decode(&self, _: &[u8], _: ReadRequest, schema: SchemaRef) -> Result<RecordBatch> {
        let (rows, schema) = match self {
            Self::Empty => (0, schema),
            Self::MultipleRows => (2, schema),
            Self::WrongSchema => (
                1,
                Arc::new(
                    Schema::empty().with_metadata(std::collections::HashMap::from([(
                        "unexpected".into(),
                        "metadata".into(),
                    )])),
                ),
            ),
            Self::DecodeError => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "source_decode",
                    "fixture error",
                ));
            }
        };
        RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|error| Error::new(ErrorKind::Decode, "fixture", error.to_string()))
    }
}

#[tokio::test]
async fn rejects_invalid_output_and_preserves_decoder_errors() {
    for (decoder, kind) in [
        (InvalidDocument::Empty, ErrorKind::Decode),
        (InvalidDocument::MultipleRows, ErrorKind::Decode),
        (InvalidDocument::WrongSchema, ErrorKind::Decode),
        (InvalidDocument::DecodeError, ErrorKind::Unsupported),
    ] {
        let mut reader = DocumentAccessor::new(
            MemoryAccessor::new(&fixture!("bytes", "empty.bin").await, 1),
            decoder,
        )
        .open(ReadRequest::new("empty"))
        .await
        .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), kind);
        if kind == ErrorKind::Unsupported {
            assert_eq!(error.operation(), "source_decode");
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}
