use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{RecordBatchOptions, cast::AsArray};
use arrow_schema::{DataType, Field};
use open_arrow_access::{
    Accessor, ByteReadOptions, DocumentAccessor, DocumentDecoder, Error, ErrorKind, ReadOptions,
    ReadRequest, RecordBatch, RecordBatchBuilder, Result, Schema, SchemaRef, async_trait,
};
use open_arrow_access_test_support::MemoryAccessor;

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

#[tokio::test]
async fn reads_once_lazily_with_byte_options_projection_and_owned_state() {
    for projection in [None, Some(vec![])] {
        let source = MemoryAccessor::new(&[0, 255, 128], 1);
        let observed = source.observations.clone();
        let accessor = DocumentAccessor::new(source, RawDocument, 3);
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
async fn rejects_bad_projection_and_limits_and_preserves_source_errors() {
    let source = MemoryAccessor::new(b"input", 1);
    let observed = source.observations.clone();
    let accessor = DocumentAccessor::new(source, RawDocument, 4);
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
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());

    let source = MemoryAccessor::new(b"input", 1).with_failure(0, ErrorKind::PermissionDenied);
    let mut reader = DocumentAccessor::new(source, RawDocument, 5)
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
        let mut reader = DocumentAccessor::new(MemoryAccessor::new(b"", 1), decoder, 0)
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
