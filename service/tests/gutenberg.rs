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

mod file_concurrency {
    use super::*;
    use open_arrow_access_core::{
        ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Reader, Result, async_trait,
    };
    use open_arrow_access_service::gutenberg::{GutenbergBookDecoder, GutenbergBooks};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct Observations {
        active: AtomicUsize,
        peak: AtomicUsize,
        completed: Mutex<Vec<String>>,
    }

    struct Files(Arc<Observations>);
    struct File {
        inner: Box<dyn ByteReader>,
        observed: Arc<Observations>,
        uri: String,
        finished: bool,
    }

    impl Drop for File {
        fn drop(&mut self) {
            self.observed.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Accessor for Files {
        type Request = ByteReadRequest;
        type Reader = dyn ByteReader;

        fn capability(&self) -> Capability {
            MemoryAccessor::new(b"", 1).capability()
        }

        async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
            let uri = request.object.locator().to_owned();
            let inner = MemoryAccessor::new(uri.as_bytes(), 8).open(request).await?;
            let active = self.0.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.peak.fetch_max(active, Ordering::SeqCst);
            Ok(Box::new(File {
                inner,
                observed: self.0.clone(),
                uri,
                finished: false,
            }))
        }
    }

    #[async_trait]
    impl Reader for File {
        type Item = Bytes;
        type Metadata = ByteMetadata;
        type Capability = Capability;

        fn capability(&self) -> Capability {
            self.inner.capability()
        }
        fn metadata(&self) -> &ByteMetadata {
            self.inner.metadata()
        }

        async fn next(&mut self) -> Result<Option<Bytes>> {
            // Deliberately complete ZIP before TXT without wall-clock sleeps.
            for _ in 0..if self.uri.ends_with(".zip") { 1 } else { 6 } {
                tokio::task::yield_now().await;
            }
            let result = self.inner.next().await?;
            if result.is_none() && !self.finished {
                self.finished = true;
                self.observed
                    .completed
                    .lock()
                    .unwrap()
                    .push(self.uri.clone());
            }
            Ok(result)
        }
    }

    #[async_trait]
    impl ByteReader for File {}

    #[rstest::rstest]
    #[tokio::test]
    async fn bounded_reads_keep_original_order_and_duplicates(#[values(1, 2, 4)] limit: usize) {
        let raw = fixture!("gutenberg", "book.rdf").await;
        let observed = Arc::new(Observations::default());
        let mut decoder = GutenbergBookDecoder::new(Files(observed.clone()));
        // Exercise the default sequential behavior as well as explicit limits.
        if limit != 1 {
            decoder = decoder.with_file_concurrency(NonZeroUsize::new(limit).unwrap());
        }
        let books = GutenbergBooks::new(MemoryAccessor::new(&raw, 17), decoder);
        let mut reader = books.open(request()).await.unwrap();
        assert_eq!(observed.active.load(Ordering::SeqCst), 0);
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch["raw_rdf"].as_binary::<i64>().value(0), raw);
        let files = batch["files"].as_list::<i32>().value(0);
        let entries = files.as_struct();
        let uris = [
            "https://example.invalid/42.txt",
            "https://example.invalid/42.zip",
            "https://example.invalid/42.txt",
        ];
        assert_eq!(entries.len(), uris.len());
        for (index, uri) in uris.iter().enumerate() {
            assert_eq!(entries["uri"].as_string::<i32>().value(index), *uri);
            assert_eq!(
                entries["data"].as_binary::<i64>().value(index),
                uri.as_bytes()
            );
        }
        assert_eq!(observed.peak.load(Ordering::SeqCst), limit.min(3));
        assert_eq!(observed.active.load(Ordering::SeqCst), 0);
        let completed = observed.completed.lock().unwrap();
        assert_eq!(completed.len(), 3);
        assert_eq!(completed[0], uris[if limit == 1 { 0 } else { 1 }]);
    }

    #[tokio::test]
    async fn dropping_pending_book_drops_all_file_readers() {
        let raw = fixture!("gutenberg", "book.rdf").await;
        let observed = Arc::new(Observations::default());
        let books = GutenbergBooks::new(
            MemoryAccessor::new(&raw, 17),
            GutenbergBookDecoder::new(Files(observed.clone()))
                .with_file_concurrency(NonZeroUsize::new(2).unwrap()),
        );
        let mut reader = books.open(request()).await.unwrap();
        let mut reading = Box::pin(reader.next());
        assert!(futures::poll!(reading.as_mut()).is_pending());
        assert_eq!(observed.active.load(Ordering::SeqCst), 2);
        drop(reading);
        drop(reader);
        assert_eq!(observed.active.load(Ordering::SeqCst), 0);
    }
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

#[rstest::rstest]
#[tokio::test]
async fn books_propagate_payload_errors(#[values(1, 2, 4)] concurrency: usize) {
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
            GutenbergBookDecoder::new(files)
                .with_file_concurrency(NonZeroUsize::new(concurrency).unwrap()),
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

#[tokio::test]
async fn mirror_books_keep_advertised_uris_and_payload_bytes() {
    use open_arrow_access_service::gutenberg::{
        GutenbergBookDecoder, GutenbergBooks, GutenbergMirror,
    };
    let raw = String::from_utf8(fixture!("gutenberg", "book.rdf").await)
        .unwrap()
        .replace(
            "https://example.invalid/42.txt",
            "https://www.gutenberg.org/files/42/42-0.txt",
        )
        .replace(
            "https://example.invalid/42.zip",
            "https://www.gutenberg.org/ebooks/42.epub.images",
        );
    let files = MemoryAccessor::new(b"\x00\xfforiginal bytes", 3);
    let observed = files.observations.clone();
    let books = GutenbergBooks::new(
        MemoryAccessor::new(raw.as_bytes(), 17),
        GutenbergBookDecoder::new(GutenbergMirror::new(files, "snapshot/")),
    );
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    };
    let mut reader = books
        .open(request().with_byte_options(options))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch["raw_rdf"].as_binary::<i64>().value(0), raw.as_bytes());
    let files = batch["files"].as_list::<i32>().value(0);
    let files = files.as_struct();
    assert_eq!(
        files["uri"].as_string::<i32>().value(0),
        "https://www.gutenberg.org/files/42/42-0.txt"
    );
    assert_eq!(
        files["uri"].as_string::<i32>().value(1),
        "https://www.gutenberg.org/ebooks/42.epub.images"
    );
    for bytes in files["data"].as_binary::<i64>().iter().flatten() {
        assert_eq!(bytes, b"\x00\xfforiginal bytes");
    }
    let requests = observed.requests.lock().unwrap();
    let expected = [
        "snapshot/4/42/42-0.txt",
        "snapshot/cache/epub/42/pg42-images.epub",
        "snapshot/4/42/42-0.txt",
    ];
    assert_eq!(requests.len(), expected.len());
    for (request, path) in requests.iter().zip(expected) {
        assert_eq!(request.object.locator(), path);
        assert_eq!(request.options, options);
    }
}

#[tokio::test]
async fn mirror_preserves_sequential_open_and_metadata_overrides() {
    use open_arrow_access_core::{
        ByteMetadata, ByteReadRequest, ByteReader, Capability, Error, Result, async_trait,
    };
    use open_arrow_access_service::gutenberg::GutenbergMirror;

    // Implements Accessor alone: mirror routing does not need backend discovery.
    struct SequentialOnly(MemoryAccessor);

    #[async_trait]
    impl Accessor for SequentialOnly {
        type Request = ByteReadRequest;
        type Reader = dyn ByteReader;

        fn capability(&self) -> Capability {
            self.0.capability()
        }

        async fn metadata(&self, request: ByteReadRequest) -> Result<ByteMetadata> {
            self.0.metadata(request).await
        }

        async fn inspect_metadata(&self, _: ByteReadRequest) -> Result<Option<ByteMetadata>> {
            Ok(None)
        }

        async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "test",
                "use sequential open",
            ))
        }

        async fn open_sequential(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
            self.0.open(request).await
        }
    }

    let files = MemoryAccessor::new(b"bytes", 5)
        .with_ranges()
        .with_listing();
    let mirror = GutenbergMirror::new(SequentialOnly(files), "");
    let request = ByteReadRequest::new("https://www.gutenberg.org/cache/epub/42/pg42.txt");
    assert!(!mirror.capability().list);
    assert!(mirror.capability().read_with_range);
    let metadata = mirror.metadata(request.clone()).await.unwrap();
    assert_eq!(metadata.source.unwrap().locator(), "cache/epub/42/pg42.txt");
    assert!(
        mirror
            .inspect_metadata(request.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(mirror.open(request.clone()).await.is_err());
    let mut reader = mirror.open_sequential(request).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"bytes");
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "cache/epub/42/pg42.txt"
    );
}
