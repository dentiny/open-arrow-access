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
async fn retains_conflicting_file_declarations_in_metadata_and_books() {
    use open_arrow_access_service::gutenberg::{GutenbergBookDecoder, GutenbergBooks};
    let raw = fixture!("gutenberg", "repeated_file_properties.rdf").await;
    let mut metadata = GutenbergAccessor::with_default_decoder(MemoryAccessor::new(&raw, 17))
        .open(request())
        .await
        .unwrap();
    let batch = metadata.next().await.unwrap().unwrap();
    let files = batch["files"].as_list::<i32>().value(0);
    let files = files.as_struct();
    assert!(files.column_by_name("extent").is_none());
    assert!(files.column_by_name("modified").is_none());
    let extents = files["extent_values"].as_list::<i32>().value(0);
    assert_eq!(
        extents.as_primitive::<UInt64Type>().values().as_ref(),
        &[402223, 402145]
    );
    let modified = files["modified_values"].as_list::<i32>().value(0);
    assert_eq!(
        modified.as_string::<i32>().value(0),
        "2024-11-05T04:46:05.484723"
    );
    assert_eq!(
        modified.as_string::<i32>().value(1),
        "2023-10-05T05:40:29.012693"
    );
    assert!(files["extent_values"].as_list::<i32>().value(1).is_empty());
    let books = GutenbergBooks::new(
        MemoryAccessor::new(&raw, 17),
        GutenbergBookDecoder::new(MemoryAccessor::new(b"payload", 2)),
    );
    let mut reader = books.open(request()).await.unwrap();
    let book = reader.next().await.unwrap().unwrap();
    assert_eq!(book["raw_rdf"].as_binary::<i64>().value(0), raw);
    let contents = book["files"].as_list::<i32>().value(0);
    assert_eq!(
        contents.as_struct()["data"].as_binary::<i64>().value(0),
        b"payload"
    );
    assert_eq!(
        contents.as_struct()["extent_values"]
            .as_list::<i32>()
            .value(0)
            .as_ref(),
        extents.as_ref()
    );
}

#[tokio::test]
async fn retains_duplicate_list_values_and_validates_every_numeric_declaration() {
    let raw = String::from_utf8(fixture!("gutenberg", "book.rdf").await).unwrap();
    let agreed = raw.replace(
        "<d:extent>7</d:extent>",
        "<d:extent>7</d:extent><d:extent>7</d:extent>",
    );
    let mut reader =
        GutenbergAccessor::with_default_decoder(MemoryAccessor::new(agreed.as_bytes(), 17))
            .open(request())
            .await
            .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    let files = batch["files"].as_list::<i32>().value(0);
    assert_eq!(
        files.as_struct()["extent_values"]
            .as_list::<i32>()
            .value(0)
            .as_primitive::<UInt64Type>()
            .values()
            .as_ref(),
        &[7, 7]
    );
    let bad = raw.replace(
        "<d:extent>7</d:extent>",
        "<d:extent>7</d:extent><d:extent>invalid</d:extent>",
    );
    let mut reader =
        GutenbergAccessor::with_default_decoder(MemoryAccessor::new(bad.as_bytes(), 17))
            .open(request().with_options(ReadOptions {
                projection: Some(vec!["title".into()]),
                ..Default::default()
            }))
            .await
            .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
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

// Files are opaque bytes: test binary/empty values and advertised URI/MIME
// retention rather than repeating the same reader test for each file extension.
#[tokio::test]
async fn books_keep_original_bytes_rdf_and_file_order() {
    use open_arrow_access_service::gutenberg::{GutenbergBookDecoder, GutenbergBooks, book_schema};
    let raw = String::from_utf8(fixture!("gutenberg", "book.rdf").await)
        .unwrap()
        .replace("application/zip", "application/x-future-format")
        .into_bytes();
    for payload in [b"\x00\xff\xfe".as_slice(), b""] {
        let rdf = MemoryAccessor::new(&raw, 17);
        let rdf_observed = rdf.observations.clone();
        let files = MemoryAccessor::new(payload, 2);
        let observed = files.observations.clone();
        let books = GutenbergBooks::new(rdf, GutenbergBookDecoder::new(files));
        let options = ByteReadOptions {
            io_concurrency: NonZeroUsize::new(2).unwrap(),
            ..Default::default()
        };
        let mut reader = open_arrow_access_test_support::assert_metadata_matches_open(
            &books,
            request().with_byte_options(options),
        )
        .await;
        assert_eq!(reader.metadata().schema, book_schema());
        assert!(rdf_observed.requests.lock().unwrap().is_empty());
        assert!(observed.requests.lock().unwrap().is_empty());
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch["raw_rdf"].as_binary::<i64>().value(0), raw);
        let entries = batch["files"].as_list::<i32>().value(0);
        let entries = entries.as_struct();
        let requests = observed.requests.lock().unwrap();
        let uris = [
            "https://example.invalid/42.txt",
            "https://example.invalid/42.zip",
            "https://example.invalid/42.txt",
        ];
        assert_eq!(entries.len(), uris.len());
        assert_eq!(requests.len(), uris.len());
        for (i, uri) in uris.into_iter().enumerate() {
            assert_eq!(entries["uri"].as_string::<i32>().value(i), uri);
            assert_eq!(entries["data"].as_binary::<i64>().value(i), payload);
            assert_eq!(requests[i].object.locator(), uri);
            assert_eq!(requests[i].options, options);
        }
        drop(requests);
        let formats = entries["format"].as_list::<i32>().value(1);
        assert_eq!(
            formats.as_string::<i32>().iter().collect::<Vec<_>>(),
            vec![Some("application/x-future-format"), Some("text/plain")]
        );
        assert_eq!(rdf_observed.requests.lock().unwrap()[0].options, options);
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn books_skip_unselected_or_absent_files() {
    use open_arrow_access_service::gutenberg::{GutenbergBookDecoder, GutenbergBooks};
    for (fixture_name, projection) in [
        ("book.rdf", Some(vec!["raw_rdf".into(), "ebook_id".into()])),
        ("book.rdf", Some(vec![])),
        ("metadata_only.rdf", None),
    ] {
        let raw = fixture!("gutenberg", fixture_name).await;
        let files = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::NotFound);
        let observed = files.observations.clone();
        let books = GutenbergBooks::new(
            MemoryAccessor::new(&raw, 17),
            GutenbergBookDecoder::new(files),
        );
        let options = ReadOptions {
            projection: projection.clone(),
            ..Default::default()
        };
        let mut reader = books.open(request().with_options(options)).await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        if let Some(columns) = projection {
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>(),
                columns.iter().collect::<Vec<_>>()
            );
        } else {
            assert!(batch["files"].as_list::<i32>().value(0).is_empty());
        }
        assert!(observed.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn books_propagate_payload_errors() {
    use open_arrow_access_service::gutenberg::{GutenbergBookDecoder, GutenbergBooks};
    let raw = fixture!("gutenberg", "book.rdf").await;
    for (files, expected) in [
        (
            MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::NotFound),
            ErrorKind::NotFound,
        ),
        (
            MemoryAccessor::new(b"partial", 1).with_failure(2, ErrorKind::Io),
            ErrorKind::Io,
        ),
    ] {
        let books = GutenbergBooks::new(
            MemoryAccessor::new(&raw, 17),
            GutenbergBookDecoder::new(files),
        );
        let mut reader = books.open(request()).await.unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), expected);
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn books_validate_projection_before_io_and_rdf_before_files() {
    use open_arrow_access_service::gutenberg::{GutenbergBookDecoder, GutenbergBooks};
    let raw = fixture!("gutenberg", "bad_date.rdf").await;
    let rdf = MemoryAccessor::new(&raw, 17);
    let rdf_observed = rdf.observations.clone();
    let files = MemoryAccessor::new(b"content", 3);
    let files_observed = files.observations.clone();
    let books = GutenbergBooks::new(rdf, GutenbergBookDecoder::new(files));
    open_arrow_access_test_support::expect_err_kind(
        books.open(request().with_projection(["missing"])).await,
        ErrorKind::InvalidInput,
    );
    assert!(rdf_observed.requests.lock().unwrap().is_empty());
    let mut reader = books
        .open(request().with_projection(["ebook_id"]))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(files_observed.requests.lock().unwrap().is_empty());
}
