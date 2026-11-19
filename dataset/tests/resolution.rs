use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, AccessorExt, ByteReadOptions, ErrorKind, ReadOptions};
use open_arrow_access_dataset::{
    directory::{DirectoryRequest, resolve_directory},
    file_set::FileSetRequest,
    resolution::{
        DetectedFormat, FormatPolicy, JsonDetection, ResolvedFileFormat as Format, resolve_files,
    },
};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fs_operator};

fn storage(files: &[(&str, &[u8])]) -> (tempfile::TempDir, OpenDalAccessor) {
    let directory = tempfile::tempdir().unwrap();
    for (name, bytes) in files {
        let path = directory.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    let storage = OpenDalAccessor::new(fs_operator(directory.path()));
    (directory, storage)
}

fn names(request: &FileSetRequest) -> Vec<&str> {
    request
        .objects
        .iter()
        .map(|object| object.locator())
        .collect()
}

#[tokio::test]
async fn preferences_are_opt_in_and_ignore_sidecars_without_opening_them() {
    let storage = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::PermissionDenied);
    let request = FileSetRequest::new(["sidecar.json", "b.PARQUET", "a.parquet", "README"]);
    let error = resolve_files(&storage, request.clone(), &FormatPolicy::default())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("sidecar.json"));
    assert!(error.to_string().contains("b.PARQUET"));
    let policy = FormatPolicy {
        preferred: vec![Format::Parquet],
        ..Default::default()
    };
    let resolved = resolve_files(&storage, request, &policy).await.unwrap();
    assert_eq!(resolved.format, Format::Parquet);
    assert_eq!(names(&resolved.request), ["b.PARQUET", "a.parquet"]);
    assert_eq!(resolved.ignored.len(), 2);
    assert!(storage.observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn csv_and_tsv_share_a_family_and_explicit_order_options_and_duplicates_survive() {
    let storage = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::Io);
    let request = FileSetRequest::new(["b.tsv.GZ", "a.csv", "a.csv"])
        .with_options(ReadOptions {
            batch_size: NonZeroUsize::new(7).unwrap(),
            projection: Some(vec!["id".into()]),
        })
        .with_byte_options(ByteReadOptions {
            io_concurrency: NonZeroUsize::new(3).unwrap(),
            io_chunk_size: NonZeroUsize::new(17),
        });
    let resolved = resolve_files(&storage, request, &FormatPolicy::default())
        .await
        .unwrap();
    assert_eq!(resolved.format, Format::Delimited);
    assert_eq!(names(&resolved.request), ["b.tsv.GZ", "a.csv", "a.csv"]);
    assert_eq!(resolved.request.options.batch_size.get(), 7);
    assert_eq!(resolved.request.options.projection.unwrap(), ["id"]);
    assert_eq!(resolved.request.byte_options.io_concurrency.get(), 3);
    assert_eq!(
        resolved.request.byte_options.io_chunk_size.unwrap().get(),
        17
    );
    assert!(storage.observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn allowlists_and_empty_selections_have_explicit_results() {
    let storage = MemoryAccessor::new(b"", 1);
    let policy = FormatPolicy {
        allowed: vec![DetectedFormat::Xml],
        ..Default::default()
    };
    let resolved = resolve_files(
        &storage,
        FileSetRequest::new(["data.csv", "doc.XML.GZ", "README"]),
        &policy,
    )
    .await
    .unwrap();
    assert_eq!(resolved.format, Format::Xml);
    assert_eq!(names(&resolved.request), ["doc.XML.GZ"]);
    for request in [
        FileSetRequest::new(["README", "data.csv"]),
        FileSetRequest::new(Vec::<String>::new()),
    ] {
        assert_eq!(
            resolve_files(&storage, request, &policy)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
    }
    let policy = FormatPolicy {
        preferred: vec![Format::Atif],
        ..Default::default()
    };
    assert_eq!(
        resolve_files(&storage, FileSetRequest::new(["doc.json"]), &policy)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn directory_resolution_reuses_glob_suffix_recursion_and_sorting() {
    let (_dir, storage) = storage(&[
        ("data/z.csv", b""),
        ("data/a.tsv", b""),
        ("data/nested/b.csv", b""),
        ("data/README", b""),
    ]);
    let request = DirectoryRequest::new("data/**/*");
    let resolved = resolve_files(&storage, request, &FormatPolicy::default())
        .await
        .unwrap();
    assert_eq!(resolved.format, Format::Delimited);
    assert_eq!(
        names(&resolved.request),
        ["data/a.tsv", "data/nested/b.csv", "data/z.csv"]
    );
    assert_eq!(resolved.ignored[0].locator(), "data/README");
    let mut request = DirectoryRequest::new("data");
    request.recursive = false;
    request.suffix = Some(".csv".into());
    let resolved = resolve_directory(&storage, request).await.unwrap();
    assert_eq!(names(&resolved), ["data/z.csv"]);
}

#[tokio::test]
async fn json_probe_distinguishes_pretty_atif_and_jsonl_without_treating_empty_files_as_votes() {
    let (_dir, storage) = storage(&[
        ("empty.json", b" \n\t"),
        (
            "pretty.json",
            b"{\n  \"schema_version\": \"ATIF-1.5\",\n  \"id\": 1\n}",
        ),
        (
            "records.jsonl",
            b"{\"schema_version\":\"ATIF-1.5\",\"id\":2}\n{}\n",
        ),
        ("ordinary.ndjson", b"{\"id\":1}\n"),
    ]);
    let resolved = resolve_files(
        &storage,
        FileSetRequest::new(["empty.json", "pretty.json", "records.jsonl"]),
        &FormatPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(resolved.format, Format::Atif);
    assert_eq!(
        names(&resolved.request),
        ["empty.json", "pretty.json", "records.jsonl"]
    );
    let error = resolve_files(
        &storage,
        FileSetRequest::new(["pretty.json", "ordinary.ndjson"]),
        &FormatPolicy::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    for name in ["pretty.json", "ordinary.ndjson", "Atif", "Json"] {
        assert!(error.to_string().contains(name));
    }
    let first_only = FormatPolicy {
        json_detection: JsonDetection::FirstNonEmpty,
        ..Default::default()
    };
    assert_eq!(
        resolve_files(
            &storage,
            FileSetRequest::new(["empty.json", "pretty.json", "ordinary.ndjson"]),
            &first_only
        )
        .await
        .unwrap()
        .format,
        Format::Atif
    );
    let empty = resolve_files(
        &storage,
        FileSetRequest::new(["empty.json"]),
        &FormatPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(empty.format, Format::Json);
    assert_eq!(names(&empty.request), ["empty.json"]);
}

#[tokio::test]
async fn disabled_json_probing_preserves_signed_urls_without_io() {
    let storage = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::PermissionDenied);
    let locator = "https://example.test/data.ndjson.gz?download=1#fragment";
    let policy = FormatPolicy {
        json_detection: JsonDetection::Disabled,
        ..Default::default()
    };
    let resolved = resolve_files(&storage, FileSetRequest::new([locator]), &policy)
        .await
        .unwrap();
    assert_eq!(resolved.format, Format::Json);
    assert_eq!(names(&resolved.request), [locator]);
    assert!(storage.observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn probe_is_bounded_replays_no_data_and_retains_io_options() {
    let policy = FormatPolicy {
        max_probe_bytes: NonZeroUsize::new(64).unwrap(),
        ..Default::default()
    };
    for chunk in [1, 4096] {
        let storage = MemoryAccessor::new(
            b"{\"schema_version\":\"ATIF-1\"}\ntrailing data is not validated",
            chunk,
        );
        let resolved = resolve_files(&storage, FileSetRequest::new(["record.json"]), &policy)
            .await
            .unwrap();
        assert_eq!(resolved.format, Format::Atif);
        assert_eq!(
            storage.observations.requests.lock().unwrap()[0]
                .options
                .io_chunk_size
                .unwrap()
                .get(),
            64
        );
    }
    let storage = MemoryAccessor::new(&vec![b' '; 1024], 1);
    let error = resolve_files(&storage, FileSetRequest::new(["huge.json"]), &policy)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("huge.json"));
    assert!(
        storage
            .observations
            .reads
            .load(std::sync::atomic::Ordering::SeqCst)
            <= 64
    );
    let exact = MemoryAccessor::new(b"{}", 1);
    let policy = FormatPolicy {
        max_probe_bytes: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    };
    assert_eq!(
        resolve_files(&exact, FileSetRequest::new(["a.json"]), &policy)
            .await
            .unwrap()
            .format,
        Format::Json
    );
}

#[tokio::test]
async fn source_errors_and_invalid_byte_chunks_are_not_hidden() {
    for storage in [
        MemoryAccessor::new(b"{}", 1).with_open_failure(ErrorKind::PermissionDenied),
        MemoryAccessor::new(b"{}", 1).with_failure(1, ErrorKind::PermissionDenied),
    ] {
        assert_eq!(
            resolve_files(
                &storage,
                FileSetRequest::new(["record.json"]),
                &FormatPolicy::default()
            )
            .await
            .unwrap_err()
            .kind(),
            ErrorKind::PermissionDenied
        );
    }
    let storage = MemoryAccessor::new(b"{}", 1).with_empty_chunk(0);
    assert_eq!(
        resolve_files(
            &storage,
            FileSetRequest::new(["record.json"]),
            &FormatPolicy::default()
        )
        .await
        .unwrap_err()
        .kind(),
        ErrorKind::Decode
    );
}

#[tokio::test]
async fn json_syntax_validation_stays_with_the_reader_including_nonfinite_extensions() {
    for bytes in [b"{\"value\":NaN}".as_slice(), b"{broken}", b"{\"value\":"] {
        let storage = MemoryAccessor::new(bytes, 1);
        assert_eq!(
            resolve_files(
                &storage,
                FileSetRequest::new(["record.json"]),
                &FormatPolicy::default()
            )
            .await
            .unwrap()
            .format,
            Format::Json
        );
    }
}

#[tokio::test]
async fn compressed_json_resolves_and_the_selection_can_be_read_by_existing_accessors() {
    use open_arrow_access_dataset::file_set::FileSetAccessor;
    use open_arrow_access_decompression::AutoDecompressionLayer;
    use open_arrow_access_file_format::json::JsonAccessor;
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"{\"id\":1}\n{\"id\":2}\n").unwrap();
    let bytes = encoder.finish().unwrap();
    let (_dir, storage) = storage(&[("records.jsonl.gz", &bytes)]);
    let resolved = resolve_files(
        &storage,
        FileSetRequest::new(["records.jsonl.gz"]),
        &FormatPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(resolved.format, Format::Json);
    let accessor = FileSetAccessor::new(JsonAccessor::infer(
        storage.layer(AutoDecompressionLayer::new()),
    ));
    let metadata = accessor.metadata(resolved.request.clone()).await.unwrap();
    let mut reader = accessor.open(resolved.request).await.unwrap();
    assert_eq!(reader.metadata(), &metadata);
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn resolved_csv_tsv_selection_uses_existing_inference_and_parallel_planning() {
    use open_arrow_access_dataset::file_set::FileSetAccessor;
    use open_arrow_access_file_format::csv::{CsvAccessor, CsvHeader};
    use open_arrow_access_layers::ParallelReadLayer;
    let (_dir, storage) = storage(&[("b.tsv", b"id\n2\n"), ("a.csv", b"id\n1\n")]);
    let resolved = resolve_files(
        &storage,
        DirectoryRequest::new(""),
        &FormatPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(resolved.format, Format::Delimited);
    let accessor =
        FileSetAccessor::new(CsvAccessor::infer(storage).with_header(CsvHeader::Present))
            .with_file_planning()
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ));
    let mut reader = accessor.open(resolved.request).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn compressed_signed_locators_keep_the_original_storage_request() {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(b"{\"schema_version\":\"ATIF-1\"}")
        .unwrap();
    let storage = MemoryAccessor::new(&encoder.finish().unwrap(), 1);
    let locator = "https://example.test/records.json.gz?download=1#fragment";
    let resolved = resolve_files(
        &storage,
        FileSetRequest::new([locator]),
        &FormatPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(resolved.format, Format::Atif);
    assert_eq!(names(&resolved.request), [locator]);
    assert_eq!(
        storage.observations.requests.lock().unwrap()[0]
            .object
            .locator(),
        locator
    );
}
