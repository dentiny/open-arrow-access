use std::num::NonZeroUsize;

use arrow_array::{
    Array,
    cast::AsArray,
    types::{Date32Type, Int32Type, UInt64Type},
};
use open_arrow_access_core::{Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest};
use open_arrow_access_service::gutenberg::{GutenbergAccessor, schema};
use open_arrow_access_test_support::{MemoryAccessor, fixture};

fn request() -> ReadRequest {
    ReadRequest::new("book.rdf")
}

#[tokio::test]
async fn keeps_mapped_book_and_file_metadata_in_rdf_order() {
    let accessor = GutenbergAccessor::with_default_decoder(MemoryAccessor::new(
        &fixture!("gutenberg", "book.rdf").await,
        17,
    ));
    let mut reader = accessor.open(request()).await.unwrap();
    assert_eq!(reader.metadata().schema, schema());
    assert_eq!(reader.metadata().schema.fields().len(), 9);
    assert!(!accessor.capability().list);
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch["ebook_id"].as_primitive::<UInt64Type>().value(0), 42);
    assert_eq!(batch["title"].as_string::<i64>().value(0), "A & B");
    assert_eq!(batch["issued"].as_primitive::<Date32Type>().value(0), 1);
    let creators = batch["creator"].as_list::<i32>().value(0);
    let creators = creators.as_struct();
    assert_eq!(
        creators["birthdate"].as_primitive::<Int32Type>().value(0),
        -20
    );
    let files = batch["files"].as_list::<i32>().value(0);
    let files = files.as_struct();
    let uris = files["uri"].as_string::<i32>();
    assert_eq!(uris.value(0), "https://example.invalid/42.txt");
    assert_eq!(uris.value(1), "https://example.invalid/42.zip");
    assert_eq!(uris.len(), 3);
    assert_eq!(uris.value(2), uris.value(0));
    assert_eq!(files.num_columns(), 4);
    assert_eq!(files["format"].as_list::<i32>().value(0).len(), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn projection_reads_only_rdf_and_preserves_zero_column_rows() {
    for columns in [
        vec!["title".to_owned(), "ebook_id".to_owned()],
        vec!["files".to_owned()],
        vec![],
    ] {
        let bytes = MemoryAccessor::new(&fixture!("gutenberg", "book.rdf").await, 11);
        let observations = bytes.observations.clone();
        let accessor = GutenbergAccessor::with_default_decoder(bytes);
        let options = ByteReadOptions {
            io_concurrency: NonZeroUsize::new(2).unwrap(),
            ..Default::default()
        };
        let mut reader = accessor
            .open(
                request()
                    .with_options(ReadOptions {
                        projection: Some(columns.clone()),
                        ..Default::default()
                    })
                    .with_byte_options(options),
            )
            .await
            .unwrap();
        assert_eq!(
            reader
                .metadata()
                .schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            columns
        );
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), columns.len());
        let requests = observations.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].options, options);
    }
}

#[tokio::test]
async fn rejects_invalid_rdf_without_silently_dropping_rows() {
    for name in [
        "bad_ebook_id.rdf",
        "bad_date.rdf",
        "bad_file_uri.rdf",
        "bad_namespace.rdf",
        "bad_multiple_books.rdf",
    ] {
        let raw = fixture!("gutenberg", name).await;
        let accessor = GutenbergAccessor::with_default_decoder(MemoryAccessor::new(&raw, 19));
        let mut reader = accessor.open(request()).await.unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn keeps_metadata_only_books_with_missing_optional_fields() {
    let raw = fixture!("gutenberg", "metadata_only.rdf").await;
    let accessor = GutenbergAccessor::with_default_decoder(MemoryAccessor::new(&raw, 13));
    let mut reader = accessor.open(ReadRequest::new("book.rdf")).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch["title"].is_null(0));
    assert!(batch["files"].as_list::<i32>().value(0).is_empty());
}

#[tokio::test]
async fn rdf_metadata_can_exceed_the_previous_size_limit() {
    let mut raw = fixture!("gutenberg", "book.rdf").await;
    raw.extend(std::iter::repeat_n(b' ', 4 * 1024 * 1024));
    let mut reader = GutenbergAccessor::with_default_decoder(MemoryAccessor::new(&raw, 64 * 1024))
        .open(request())
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
}
