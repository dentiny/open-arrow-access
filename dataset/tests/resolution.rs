use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, ReadOptions};
use open_arrow_access_dataset::{
    directory::{DirectoryRequest, resolve_directory},
    file_set::{FileSetAccessor, FileSetRequest},
    resolution::{DetectedFormat, FormatPolicy, ResolvedFileFormat as Format, resolve_files},
};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fs_operator};

#[tokio::test]
async fn filename_selection_preserves_options_and_checks_mixed_and_empty_inputs_without_io() {
    let storage = MemoryAccessor::new(b"", 1).with_open_failure(ErrorKind::PermissionDenied);
    let options = ReadOptions {
        batch_size: NonZeroUsize::MIN,
        projection: Some(vec!["id".into()]),
    };
    let request =
        FileSetRequest::new(["b.tsv.GZ", "README", "a.csv", "a.csv"]).with_options(options.clone());
    let resolved = resolve_files(&storage, request, &FormatPolicy::default())
        .await
        .unwrap();
    assert_eq!(resolved.format, Format::Delimited);
    assert_eq!(
        resolved.request.objects,
        FileSetRequest::new(["b.tsv.GZ", "a.csv", "a.csv"]).objects
    );
    assert_eq!(resolved.request.options.batch_size, options.batch_size);
    assert_eq!(resolved.request.options.projection, options.projection);
    let mixed = FileSetRequest::new(["sidecar.json", "data.parquet"]);
    let error = resolve_files(&storage, mixed.clone(), &FormatPolicy::default())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("sidecar.json") && error.to_string().contains("data.parquet")
    );
    let policy = FormatPolicy {
        preferred: Some(DetectedFormat::Parquet),
        ..Default::default()
    };
    let resolved = resolve_files(&storage, mixed, &policy).await.unwrap();
    assert_eq!(resolved.format, Format::Parquet);
    assert_eq!(
        resolved.request.objects,
        FileSetRequest::new(["data.parquet"]).objects
    );
    assert_eq!(
        resolve_files(&storage, FileSetRequest::new(["README"]), &policy)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    let locator = "https://example.test/data.json.gz?token=example";
    let policy = FormatPolicy {
        json_probe_bytes: None,
        ..Default::default()
    };
    let resolved = resolve_files(&storage, FileSetRequest::new([locator]), &policy)
        .await
        .unwrap();
    assert_eq!(resolved.request.objects[0].locator(), locator);
    assert!(storage.observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn directory_glob_selection_feeds_existing_parallel_csv_readers() {
    use open_arrow_access_file_format::csv::{CsvAccessor, CsvHeader};
    use open_arrow_access_layers::ParallelReadLayer;
    let directory = tempfile::tempdir().unwrap();
    for (name, bytes) in [
        ("b.tsv", "id\n2\n"),
        ("a.csv", "id\n1\n"),
        ("README", "ignored"),
    ] {
        std::fs::write(directory.path().join(name), bytes).unwrap();
    }
    let storage = OpenDalAccessor::new(fs_operator(directory.path()));
    let request = resolve_directory(&storage, DirectoryRequest::new("*"))
        .await
        .unwrap();
    let resolved = resolve_files(&storage, request, &FormatPolicy::default())
        .await
        .unwrap();
    assert_eq!(resolved.format, Format::Delimited);
    assert_eq!(
        resolved.request.objects,
        FileSetRequest::new(["a.csv", "b.tsv"]).objects
    );
    let reader = FileSetAccessor::new(CsvAccessor::infer(storage).with_header(CsvHeader::Present))
        .with_file_planning()
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ));
    let mut reader = reader.open(resolved.request).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn json_detection_handles_empty_pretty_and_compressed_inputs_and_rejects_mixed_files() {
    use std::io::Write;
    let directory = tempfile::tempdir().unwrap();
    for (name, bytes) in [
        ("empty.json", " \n"),
        ("pretty.json", "{\n\"schema_version\":\"ATIF-1\"\n}"),
        ("plain.jsonl", "{\"id\":1}\n"),
    ] {
        std::fs::write(directory.path().join(name), bytes).unwrap();
    }
    let storage = OpenDalAccessor::new(fs_operator(directory.path()));
    for (names, expected) in [
        (["empty.json", "pretty.json"], Format::Atif),
        (["empty.json", "plain.jsonl"], Format::Json),
    ] {
        let resolved = resolve_files(
            &storage,
            FileSetRequest::new(names),
            &FormatPolicy::default(),
        )
        .await
        .unwrap();
        assert_eq!(resolved.format, expected);
        assert_eq!(resolved.request.objects.len(), 2);
    }
    let error = resolve_files(
        &storage,
        FileSetRequest::new(["pretty.json", "plain.jsonl"]),
        &FormatPolicy::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("pretty.json") && error.to_string().contains("plain.jsonl"));
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
    assert_eq!(
        storage.observations.requests.lock().unwrap()[0]
            .object
            .locator(),
        locator
    );
}

#[tokio::test]
async fn json_probes_bound_samples_and_preserve_source_errors() {
    let policy = FormatPolicy {
        json_probe_bytes: NonZeroUsize::new(64),
        ..Default::default()
    };
    for chunk in [1, 4096] {
        let storage = MemoryAccessor::new(b"{\"schema_version\":\"ATIF-1\"}\ntrailing data", chunk);
        assert_eq!(
            resolve_files(&storage, FileSetRequest::new(["data.json"]), &policy)
                .await
                .unwrap()
                .format,
            Format::Atif
        );
    }
    let storage = MemoryAccessor::new(&[b' '; 1024], 1);
    assert_eq!(
        resolve_files(&storage, FileSetRequest::new(["huge.json"]), &policy)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(
        storage
            .observations
            .reads
            .load(std::sync::atomic::Ordering::SeqCst)
            <= 64
    );
    let storage = MemoryAccessor::new(b"{}", 1).with_failure(1, ErrorKind::PermissionDenied);
    assert_eq!(
        resolve_files(&storage, FileSetRequest::new(["data.json"]), &policy)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::PermissionDenied
    );
}
