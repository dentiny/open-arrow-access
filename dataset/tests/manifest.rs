use open_arrow_access_core::{Accessor, ByteReadRequest, ErrorKind, Result};
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
    assert_eq!(
        resolve(
            manifest(),
            b"https://EXAMPLE.test:443/a.warc\n/b.warc\nc%20d.warc"
        )
        .await
        .unwrap(),
        ["a.warc", "b.warc", "c d.warc"]
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
        "https://other.test/a.warc",
        "http://example.test/a.warc",
        "https://example.test:123/a.warc",
        "https://u:p@example.test/a.warc",
        "file:///a.warc",
        "https://example.test/x/../a.warc",
        " a.warc",
        "a\\b.warc",
        "a.warc?x=1",
        "a.warc#x",
        "https://example.test/a.warc?x=1",
        "https://example.test/a.warc#x",
    ] {
        let error = resolve(manifest(), format!("\n{value}").as_bytes())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode, "{value}: {error}");
        assert!(error.to_string().contains("line 2"), "{error}");
        assert!(error.to_string().contains("lists/warc.paths"), "{error}");
    }
    assert!(
        resolve(
            PathsManifest::new("warc.paths"),
            b"https://example.test/a.warc"
        )
        .await
        .is_err()
    );
    assert_eq!(
        resolve(
            PathsManifest::new("warc.paths").with_origin("https://example.test"),
            b"https://example.test/a.warc"
        )
        .await
        .unwrap(),
        ["a.warc"]
    );
    for value in ["/a.warc", "https://example.test/a.warc"] {
        assert!(
            resolve(
                manifest().with_base(ManifestBase::ManifestDirectory),
                value.as_bytes()
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn selection_policies_preserve_order_and_detect_aliases() {
    let bytes =
        b"other/no.txt\nseg/z.WARC.GZ\nseg/no.txt\nseg/a.warc\nhttps://example.test/seg/z.WARC.GZ";
    let selection = |policy| {
        manifest()
            .with_prefix("seg/")
            .with_formats(&[DetectedFormat::Warc], policy)
    };
    assert!(
        resolve(selection(Mismatch::Error), bytes)
            .await
            .unwrap_err()
            .to_string()
            .contains("line 3")
    );
    assert!(
        resolve(selection(Mismatch::Skip), bytes)
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
    assert_eq!(
        resolve(
            selection(Mismatch::Skip).with_duplicates(Duplicates::Dedupe),
            bytes
        )
        .await
        .unwrap(),
        ["seg/z.WARC.GZ", "seg/a.warc"]
    );
    assert_eq!(
        resolve(manifest().with_prefix("absent/"), bytes)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
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
        manifest().with_origin("https://other"),
        PathsManifest::new("warc.paths").with_origin("https://host/a/.."),
        PathsManifest::new("warc.paths").with_origin("https://host?x=1"),
        PathsManifest::new("warc.paths").with_origin("https://host#x"),
        manifest().with_base(ManifestBase::Explicit("../data".into())),
    ] {
        let storage = MemoryAccessor::new(b"a.warc", 1);
        assert_eq!(
            spec.resolve(&storage).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert!(storage.observations.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn empty_invalid_utf8_and_read_errors_have_context() {
    assert_eq!(
        resolve(manifest(), b"\n\r\n").await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(
        resolve(manifest(), b"a.warc\n\xff")
            .await
            .unwrap_err()
            .to_string()
            .contains("line 2")
    );
    let storage =
        MemoryAccessor::new(b"a.warc\nb.warc", 1).with_failure(8, ErrorKind::PermissionDenied);
    let error = manifest().resolve(&storage).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(error.to_string().contains("line 2"), "{error}");
    assert!(error.to_string().contains("lists/warc.paths"), "{error}");
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
