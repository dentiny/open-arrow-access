use std::num::NonZeroUsize;

use arrow_array::{
    Array,
    cast::AsArray,
    types::{Date32Type, Int32Type, UInt64Type},
};
use open_arrow_access_core::{Accessor, ByteReadOptions, ErrorKind, ReadOptions};
use open_arrow_access_service::gutenberg::{
    GutenbergAccessor, GutenbergFile, GutenbergLimits, GutenbergRequest, schema,
};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fixture, fs_operator};

const TXT: &[u8] = b"hello\r\n";
const ZIP: &[u8] = b"PK\x03\x04\0\xff";

fn request() -> GutenbergRequest {
    GutenbergRequest::new(
        "book.rdf",
        vec![
            GutenbergFile::new("https://example.invalid/42.zip", "book.zip"),
            GutenbergFile::new("https://example.invalid/42.txt", "book.txt"),
        ],
    )
}

async fn mirror() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    for (name, data) in [
        ("book.rdf", fixture!("gutenberg", "book.rdf").await),
        ("book.txt", TXT.to_vec()),
        ("book.zip", ZIP.to_vec()),
    ] {
        tokio::fs::write(root.path().join(name), data)
            .await
            .unwrap();
    }
    root
}

#[tokio::test]
async fn keeps_book_metadata_and_original_files_in_requested_order() {
    let root = mirror().await;
    let accessor = GutenbergAccessor::new(OpenDalAccessor::new(fs_operator(root.path())));
    let mut reader = accessor.open(request()).await.unwrap();
    assert_eq!(reader.metadata().schema, schema());
    assert!(!accessor.capability().list);
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("ebook_id")
            .unwrap()
            .as_primitive::<UInt64Type>()
            .value(0),
        42
    );
    assert_eq!(
        batch
            .column_by_name("title")
            .unwrap()
            .as_string::<i64>()
            .value(0),
        "A & B"
    );
    assert_eq!(
        batch
            .column_by_name("issued")
            .unwrap()
            .as_primitive::<Date32Type>()
            .value(0),
        1
    );
    let creators = batch
        .column_by_name("creator")
        .unwrap()
        .as_list::<i32>()
        .value(0);
    let creators = creators.as_struct();
    assert_eq!(
        creators
            .column_by_name("birthdate")
            .unwrap()
            .as_primitive::<Int32Type>()
            .value(0),
        -20
    );
    let files = batch
        .column_by_name("files")
        .unwrap()
        .as_list::<i64>()
        .value(0);
    let files = files.as_struct();
    let uris = files.column_by_name("uri").unwrap().as_string::<i32>();
    assert!(uris.value(0).ends_with(".zip"));
    let data = files.column_by_name("data").unwrap().as_binary::<i64>();
    assert_eq!(data.value(0), ZIP);
    assert_eq!(data.value(1), TXT);
    assert_eq!(
        files
            .column_by_name("format")
            .unwrap()
            .as_list::<i32>()
            .value(0)
            .len(),
        2
    );
    assert_eq!(
        batch
            .column_by_name("metadata_rdf")
            .unwrap()
            .as_binary::<i64>()
            .value(0),
        fixture!("gutenberg", "book.rdf").await
    );
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn projection_skips_file_io_and_preserves_zero_column_rows() {
    for columns in [vec!["title".to_owned(), "ebook_id".to_owned()], vec![]] {
        let bytes = MemoryAccessor::new(&fixture!("gutenberg", "book.rdf").await, 11);
        let observations = bytes.observations.clone();
        let accessor = GutenbergAccessor::new(bytes);
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
async fn rejects_invalid_requests_and_rdf_without_silently_dropping_rows() {
    let raw = fixture!("gutenberg", "book.rdf").await;
    let bytes = MemoryAccessor::new(&raw, 17);
    let observations = bytes.observations.clone();
    let accessor = GutenbergAccessor::new(bytes);
    let mut duplicate = request();
    duplicate.files.push(duplicate.files[0].clone());
    assert_eq!(
        accessor.open(duplicate).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert!(observations.requests.lock().unwrap().is_empty());
    let raw = String::from_utf8(raw).unwrap();
    for invalid in [
        raw.replace("ebooks/42", "ebooks/invalid"),
        raw.replace("1970-01-02", "2026-02-30"),
        raw.replace("<p:ebook", "<p:ebook xmlns:p=\"urn:wrong\""),
        raw.replace("</r:RDF>", "<p:ebook r:about=\"ebooks/43\"/></r:RDF>"),
    ] {
        let accessor = GutenbergAccessor::new(MemoryAccessor::new(invalid.as_bytes(), 19));
        let mut reader = accessor.open(request()).await.unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        assert!(reader.next().await.unwrap().is_none());
    }
    let mut absent = request();
    absent.files[0].uri = "https://example.invalid/not-in-rdf".into();
    let mut reader = accessor.open(absent).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
}

#[tokio::test]
async fn enforces_metadata_file_and_row_limits_and_preserves_storage_errors() {
    let root = mirror().await;
    let raw_len = fixture!("gutenberg", "book.rdf").await.len();
    for limits in [
        GutenbergLimits {
            metadata_bytes: NonZeroUsize::new(raw_len - 1).unwrap(),
            ..Default::default()
        },
        GutenbergLimits {
            file_bytes: NonZeroUsize::new(ZIP.len() - 1).unwrap(),
            ..Default::default()
        },
        GutenbergLimits {
            row_bytes: NonZeroUsize::new(raw_len + ZIP.len() + TXT.len() - 1).unwrap(),
            ..Default::default()
        },
    ] {
        let accessor = GutenbergAccessor::new(OpenDalAccessor::new(fs_operator(root.path())))
            .with_limits(limits);
        let mut reader = accessor.open(request()).await.unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        assert!(reader.next().await.unwrap().is_none());
    }
    tokio::fs::remove_file(root.path().join("book.zip"))
        .await
        .unwrap();
    let accessor = GutenbergAccessor::new(OpenDalAccessor::new(fs_operator(root.path())));
    let mut reader = accessor.open(request()).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
    assert!(reader.next().await.unwrap().is_none());
    let bytes = MemoryAccessor::new(&fixture!("gutenberg", "book.rdf").await, 7)
        .with_failure(7, ErrorKind::PermissionDenied);
    let mut reader = GutenbergAccessor::new(bytes).open(request()).await.unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "memory_read");
}

#[tokio::test]
async fn keeps_metadata_only_books_with_missing_optional_fields() {
    let raw = br#"<r:RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:p="http://www.gutenberg.org/2009/pgterms/"><p:ebook r:about="ebooks/1"/></r:RDF>"#;
    let accessor = GutenbergAccessor::new(MemoryAccessor::new(raw, 13));
    let mut reader = accessor
        .open(GutenbergRequest::new("book.rdf", vec![]))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column_by_name("title").unwrap().is_null(0));
    assert!(
        batch
            .column_by_name("files")
            .unwrap()
            .as_list::<i64>()
            .value(0)
            .is_empty()
    );
}
