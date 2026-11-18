use open_arrow_access_core::{Accessor, ByteReadRequest, Error, ErrorKind, Result};
use open_arrow_access_dataset::manifest::{
    DetectedFormat, Duplicates, ManifestBase, Mismatch, PathsManifest,
};
use open_arrow_access_test_support::MemoryAccessor;

async fn resolve(spec: PathsManifest, bytes: &[u8]) -> Result<Vec<String>> {
    Ok(spec
        .resolve(&MemoryAccessor::new(bytes, 1))
        .await?
        .objects
        .into_iter()
        .map(|o| o.locator().to_owned())
        .collect())
}

fn manifest() -> PathsManifest {
    PathsManifest::new("https://example.test/lists/warc.paths")
}

fn assert_error(error: Error, kind: ErrorKind, context: &[&str]) {
    assert_eq!(error.kind(), kind, "{error}");
    assert_eq!(error.operation(), "read_paths_manifest", "{error}");
    let message = error.to_string();
    for expected in ["object \"", "warc.paths"]
        .into_iter()
        .chain(context.iter().copied())
    {
        assert!(
            message.contains(expected),
            "missing {expected:?} in {message}"
        );
    }
}

#[tokio::test]
async fn bases_produce_keys_for_the_same_storage() {
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::{collect_bytes, fs_operator};
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("lists")).unwrap();
    std::fs::create_dir(dir.path().join("data")).unwrap();
    std::fs::write(dir.path().join("lists/warc.paths"), "\r\na.warc\r\n").unwrap();
    for path in ["a.warc", "lists/a.warc", "data/a.warc"] {
        std::fs::write(dir.path().join(path), path).unwrap();
    }
    let storage = OpenDalAccessor::new(fs_operator(dir.path()));
    for (base, expected) in [
        (ManifestBase::Origin, "a.warc"),
        (ManifestBase::ManifestDirectory, "lists/a.warc"),
        (ManifestBase::Explicit("data".into()), "data/a.warc"),
    ] {
        let files = manifest().with_base(base).resolve(&storage).await.unwrap();
        assert_eq!(files.objects.len(), 1);
        let mut reader = storage
            .open(ByteReadRequest::new(files.objects[0].clone()))
            .await
            .unwrap();
        assert_eq!(
            collect_bytes(reader.as_mut()).await.unwrap(),
            expected.as_bytes()
        );
    }
}

#[tokio::test]
async fn absolute_urls_are_canonicalized_and_relative_keys_are_decoded() {
    assert_eq!(
        resolve(
            manifest(),
            b"https://EXAMPLE.test:443/a.warc\n/b.warc\nc%20d.warc"
        )
        .await
        .unwrap(),
        ["https://example.test/a.warc", "b.warc", "c d.warc"]
    );
}

#[tokio::test]
async fn unsafe_entries_report_the_manifest_and_line() {
    for value in [
        "../a.warc",
        "%2e%2e/a.warc",
        "a%2fb.warc",
        "a%252fb.warc",
        "a%GG.warc",
        "a%00.warc",
        "//other.test/a.warc",
        "//example.test:443/a.warc",
        "https://u:p@example.test/a.warc",
        "file:///a.warc",
        "mailto:user@example.test",
        "https://",
        "https://[bad]/a.warc",
        "https://example.test/x/../a.warc",
        " a.warc",
        "a\\b.warc",
    ] {
        let error = resolve(manifest(), format!("\n{value}").as_bytes())
            .await
            .expect_err(value);
        assert_error(error, ErrorKind::Decode, &["line 2"]);
    }
}

#[tokio::test]
async fn absolute_entries_are_independent_of_the_manifest_location() {
    let entries = b"https://example.test/a.warc\na.warc";
    for spec in [manifest(), PathsManifest::new("warc.paths")] {
        assert_eq!(
            resolve(spec, entries).await.unwrap(),
            ["https://example.test/a.warc", "a.warc"]
        );
    }
}

#[tokio::test]
async fn resolved_entries_must_remain_within_the_selected_base() {
    for entry in ["/a.warc", "https://example.test/a.warc"] {
        let spec = manifest().with_base(ManifestBase::ManifestDirectory);
        let error = resolve(spec, entry.as_bytes()).await.expect_err(entry);
        assert_error(error, ErrorKind::Decode, &["line 1", "configured base"]);
    }
}

#[tokio::test]
async fn selection_policies_preserve_order_and_detect_aliases() {
    let bytes = b"other/no.txt\nseg/z.WARC.GZ\nseg/no.txt\nseg/a.warc\nseg/%7a.WARC.GZ";
    let selection = |policy| {
        manifest()
            .with_prefix("seg/")
            .with_formats(&[DetectedFormat::Warc], policy)
    };
    for (policy, line, message) in [
        (
            Mismatch::Error,
            "line 3",
            "file format does not match selection",
        ),
        (Mismatch::Skip, "line 5", "duplicate selected file"),
    ] {
        let error = resolve(selection(policy), bytes).await.unwrap_err();
        assert_error(error, ErrorKind::Decode, &[line, message]);
    }
    assert_eq!(
        resolve(
            selection(Mismatch::Skip).with_duplicates(Duplicates::Dedupe),
            bytes
        )
        .await
        .unwrap(),
        ["seg/z.WARC.GZ", "seg/a.warc"]
    );
}

#[tokio::test]
async fn empty_selections_are_invalid() {
    for (spec, bytes) in [
        (manifest(), b"\n\r\n".as_slice()),
        (manifest().with_prefix("absent/"), b"a.warc".as_slice()),
    ] {
        let error = resolve(spec, bytes).await.unwrap_err();
        assert_error(error, ErrorKind::InvalidInput, &["no selected files"]);
    }
}

#[tokio::test]
async fn invalid_configuration_fails_before_io() {
    for spec in [
        PathsManifest::new("../warc.paths"),
        PathsManifest::new("warc.paths?x=1"),
        PathsManifest::new("warc.paths#x"),
        PathsManifest::new("//example.test/warc.paths"),
        PathsManifest::new("https://example.test/x/../warc.paths"),
        PathsManifest::new("https://u:p@example.test/warc.paths"),
        PathsManifest::new("https://example.test/warc.paths?x=1"),
        PathsManifest::new("https://example.test/warc.paths#x"),
        manifest().with_base(ManifestBase::Explicit("../data".into())),
    ] {
        let storage = MemoryAccessor::new(b"a.warc", 1);
        let error = spec
            .resolve(&storage)
            .await
            .expect_err(&format!("{spec:?}"));
        assert_error(error, ErrorKind::InvalidInput, &["line 1"]);
        assert!(
            storage.observations.requests.lock().unwrap().is_empty(),
            "{spec:?}"
        );
    }
}

#[tokio::test]
async fn invalid_utf8_reports_the_manifest_and_line() {
    let error = resolve(manifest(), b"a.warc\n\xff").await.unwrap_err();
    assert_error(error, ErrorKind::Decode, &["line 2", "not UTF-8"]);
}

#[tokio::test]
async fn read_errors_preserve_their_kind_and_add_context() {
    let storage =
        MemoryAccessor::new(b"a.warc\nb.warc", 1).with_failure(8, ErrorKind::PermissionDenied);
    let error = manifest().resolve(&storage).await.unwrap_err();
    assert_error(error, ErrorKind::PermissionDenied, &["line 2"]);
}

#[tokio::test]
async fn gzip_and_decoded_inputs_share_validation() {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"a.warc\n").unwrap();
    let mut bytes = encoder.finish().unwrap();
    let spec = PathsManifest::new("warc.paths.GZ");
    assert_eq!(resolve(spec.clone(), &bytes).await.unwrap(), ["a.warc"]);
    bytes.truncate(bytes.len() - 4);
    assert!(resolve(spec.clone(), &bytes).await.is_err());
    let input = MemoryAccessor::new(b"a.warc", 1)
        .open(ByteReadRequest::new("warc.paths.GZ"))
        .await
        .unwrap();
    assert_eq!(
        spec.resolve_reader(input).await.unwrap().objects[0].locator(),
        "a.warc"
    );
}

#[tokio::test]
async fn foreign_urls_support_filtering_deduplication_and_query_fragments() {
    let spec = manifest()
        .with_prefix("data/")
        .with_duplicates(Duplicates::Dedupe);
    let entries =
        b"https://mirror.test/data/%61.warc\nhttps://mirror.test/data/a.warc\ndata/b.warc?q=1#f";
    assert_eq!(
        resolve(spec, entries).await.unwrap(),
        ["https://mirror.test/data/a.warc", "data/b.warc?q=1#f"]
    );
}

#[tokio::test]
async fn foreign_http_origins_are_accepted_without_configuration() {
    for entry in [
        "https://other.test/a.warc",
        "http://example.test/a.warc",
        "https://example.test:123/a.warc",
    ] {
        assert_eq!(
            resolve(manifest(), entry.as_bytes()).await.unwrap(),
            [entry]
        );
    }
}

#[tokio::test]
async fn entry_query_and_fragment_components_are_always_preserved() {
    for (entry, expected) in [
        ("data/a.warc?x=1", "data/a.warc?x=1"),
        ("/data/a.warc#part", "data/a.warc#part"),
        (
            "https://example.test/data/a.warc?x=1#part",
            "https://example.test/data/a.warc?x=1#part",
        ),
        (
            "https://mirror.test/data/a.warc?x=1#part",
            "https://mirror.test/data/a.warc?x=1#part",
        ),
    ] {
        let spec = manifest()
            .with_prefix("data/")
            .with_formats(&[DetectedFormat::Warc], Mismatch::Error);
        assert_eq!(
            resolve(spec, entry.as_bytes()).await.unwrap(),
            [expected],
            "{entry}"
        );
    }
}

#[tokio::test]
async fn colons_in_paths_and_url_suffixes_do_not_imply_an_absolute_url() {
    for entry in [
        "data/a:b.warc",
        "data/a.warc?next=https://example.test/a:b",
        "data/a.warc#urn:part",
    ] {
        for spec in [manifest(), PathsManifest::new("lists/warc:2026.paths")] {
            assert_eq!(resolve(spec, entry.as_bytes()).await.unwrap(), [entry]);
        }
    }
}
