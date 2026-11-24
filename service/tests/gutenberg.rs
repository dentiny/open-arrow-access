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

// Distinct payloads make URI routing and byte preservation observable without
// network access or claiming to parse the individual ebook formats.
#[derive(Clone)]
struct BookFiles(std::collections::HashMap<String, MemoryAccessor>);

#[open_arrow_access_core::async_trait]
impl Accessor for BookFiles {
    type Request = open_arrow_access_core::ByteReadRequest;
    type Reader = dyn open_arrow_access_core::ByteReader;

    fn capability(&self) -> open_arrow_access_core::Capability {
        open_arrow_access_core::Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(
        &self,
        request: Self::Request,
    ) -> open_arrow_access_core::Result<Box<Self::Reader>> {
        let file = self.0.get(request.object.locator()).ok_or_else(|| {
            open_arrow_access_core::Error::new(
                ErrorKind::NotFound,
                "test_book_file",
                "missing file",
            )
        })?;
        file.open(request).await
    }
}

#[tokio::test]
async fn book_rows_preserve_every_format_original_bytes_and_unknown_rdf() {
    use open_arrow_access_service::gutenberg::{GutenbergBooks, book_schema};
    let representations: &[(&str, &str, &[u8])] = &[
        ("book.txt", "text/plain; charset=iso-8859-1", b"caf\xe9\r\n"),
        ("book.html", "text/html", b"<img src='cover.png'>"),
        ("book.zip", "application/zip", b"PK\x03\x04\x00\xff"),
        ("book.epub", "application/epub+zip", b"PK\x03\x04epub"),
        ("book-3.epub", "application/epub+zip", b"PK\x03\x04epub3"),
        (
            "book-kf8.mobi",
            "application/x-mobipocket-ebook",
            b"KF8\x00",
        ),
        ("book.mobi", "application/x-mobipocket-ebook", b"MOBI\x00"),
        ("book.pdf", "application/pdf", b"%PDF-1.7\n\xff"),
        ("book.tex", "application/x-tex", b"\\documentclass{book}"),
        ("cover.png", "image/png", b"\x89PNG\r\n"),
        ("audio.mp3", "audio/mpeg", b"ID3\x00"),
        ("future.xyz", "application/x-future-format", b"\x00\xff\xfe"),
        ("empty.txt", "text/plain", b""),
    ];
    let mut raw = String::from(
        "<r:RDF xmlns:r='http://www.w3.org/1999/02/22-rdf-syntax-ns#' xmlns:p='http://www.gutenberg.org/2009/pgterms/' xmlns:d='http://purl.org/dc/terms/'><p:ebook r:about='ebooks/42'><p:marc520>Keep this unknown field</p:marc520>",
    );
    let mut files = std::collections::HashMap::new();
    for (name, mime, data) in representations {
        let uri = format!("https://mirror.invalid/{name}");
        raw.push_str(&format!("<d:hasFormat><p:file r:about='{uri}'><d:format><r:Description><r:value>{mime}</r:value></r:Description></d:format></p:file></d:hasFormat>"));
        files.insert(uri, MemoryAccessor::new(data, 3));
    }
    raw.push_str("</p:ebook></r:RDF>");
    let files = BookFiles(files);
    let rdf = MemoryAccessor::new(raw.as_bytes(), 17);
    let rdf_observations = rdf.observations.clone();
    let accessor = GutenbergBooks::new(rdf, files.clone());
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    };
    let mut reader = open_arrow_access_test_support::assert_metadata_matches_open(
        &accessor,
        request().with_byte_options(options),
    )
    .await;
    assert_eq!(reader.metadata().schema, book_schema());
    assert!(rdf_observations.requests.lock().unwrap().is_empty());
    assert!(
        files
            .0
            .values()
            .all(|f| f.observations.requests.lock().unwrap().is_empty())
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch["raw_rdf"].as_binary::<i64>().value(0), raw.as_bytes());
    let entries = batch["files"].as_list::<i32>().value(0);
    let entries = entries.as_struct();
    assert_eq!(entries.len(), representations.len());
    for (i, (name, mime, expected)) in representations.iter().enumerate() {
        let uri = format!("https://mirror.invalid/{name}");
        assert_eq!(entries["uri"].as_string::<i32>().value(i), uri);
        let formats = entries["format"].as_list::<i32>().value(i);
        assert_eq!(formats.as_string::<i32>().value(0), *mime);
        assert_eq!(entries["data"].as_binary::<i64>().value(i), *expected);
        assert_eq!(
            files.0[&uri].observations.requests.lock().unwrap()[0].options,
            options
        );
    }
    assert_eq!(
        rdf_observations.requests.lock().unwrap()[0].options,
        options
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn book_projection_skips_payload_io_and_preserves_empty_projection() {
    use open_arrow_access_service::gutenberg::GutenbergBooks;
    for columns in [vec!["raw_rdf", "ebook_id"], vec![]] {
        let raw = fixture!("gutenberg", "book.rdf").await;
        let files = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::NotFound);
        let observed = files.observations.clone();
        let books = GutenbergBooks::new(MemoryAccessor::new(&raw, 13), files);
        let mut reader = books
            .open(request().with_projection(columns.clone()))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            columns
        );
        assert_eq!(batch.num_rows(), 1);
        assert!(observed.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn book_payload_failures_are_not_silently_dropped() {
    use open_arrow_access_service::gutenberg::GutenbergBooks;
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
        let books = GutenbergBooks::new(MemoryAccessor::new(&raw, 17), files);
        let mut reader = books.open(request()).await.unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), expected);
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn book_without_files_retains_empty_list_and_exact_rdf() {
    use open_arrow_access_service::gutenberg::GutenbergBooks;
    let raw = fixture!("gutenberg", "metadata_only.rdf").await;
    let files = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::NotFound);
    let observed = files.observations.clone();
    let books = GutenbergBooks::new(MemoryAccessor::new(&raw, 17), files);
    let mut reader = books.open(request()).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch["raw_rdf"].as_binary::<i64>().value(0), raw);
    assert!(batch["files"].as_list::<i32>().value(0).is_empty());
    assert!(observed.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn book_files_keep_duplicate_uris_multiple_mime_types_and_fail_on_later_missing_file() {
    use open_arrow_access_service::gutenberg::GutenbergBooks;
    let raw = fixture!("gutenberg", "book.rdf").await;
    let mut files = BookFiles(std::collections::HashMap::from([
        (
            "https://example.invalid/42.txt".into(),
            MemoryAccessor::new(b"text", 2),
        ),
        (
            "https://example.invalid/42.zip".into(),
            MemoryAccessor::new(b"PK\x00", 2),
        ),
    ]));
    let books = GutenbergBooks::new(MemoryAccessor::new(&raw, 17), files.clone());
    let mut reader = books
        .open(request().with_projection(["files"]))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    let entries = batch["files"].as_list::<i32>().value(0);
    let entries = entries.as_struct();
    assert_eq!(entries["data"].as_binary::<i64>().value(0), b"text");
    assert_eq!(entries["data"].as_binary::<i64>().value(1), b"PK\x00");
    assert_eq!(entries["data"].as_binary::<i64>().value(2), b"text");
    let formats = entries["format"].as_list::<i32>().value(1);
    assert_eq!(
        formats.as_string::<i32>().iter().collect::<Vec<_>>(),
        vec![Some("application/zip"), Some("text/plain")]
    );

    files.0.remove("https://example.invalid/42.zip");
    let books = GutenbergBooks::new(MemoryAccessor::new(&raw, 17), files);
    let mut reader = books.open(request()).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::NotFound);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn book_requests_validate_projection_before_io_and_rdf_before_files() {
    use open_arrow_access_service::gutenberg::GutenbergBooks;
    let raw = fixture!("gutenberg", "bad_date.rdf").await;
    let rdf = MemoryAccessor::new(&raw, 17);
    let rdf_observed = rdf.observations.clone();
    let files = MemoryAccessor::new(b"content", 3);
    let files_observed = files.observations.clone();
    let books = GutenbergBooks::new(rdf, files);
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
