use open_arrow_access_core::ErrorKind;
use open_arrow_access_dataset::{
    file_set::FileSetRequest,
    json_resolver::{JsonKind, JsonResolver, SniffConfig, SniffPolicy},
};
use open_arrow_access_file_format::atif::AtifFormat;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fs_operator};
use std::{num::NonZeroUsize, sync::Arc};
use tokio::fs;

#[tokio::test]
async fn resolves_content_types_without_reader_features() {
    for (bytes, expected) in [
        ("{\"id\":1}\n", JsonKind::Records),
        (
            "{\"schema_version\":\"ATIF-1.0\"}\n",
            JsonKind::Atif(AtifFormat::JsonLines),
        ),
        (
            "{\n  \"schema_version\": \"ATIF-1.0\"\n}\n",
            JsonKind::Atif(AtifFormat::Json),
        ),
        ("{\n  \"id\":1\n}\n", JsonKind::Records),
    ] {
        let storage = Arc::new(MemoryAccessor::new(bytes.as_bytes(), 1));
        let selected = JsonResolver::default()
            .resolve(storage, &FileSetRequest::new(["opaque.data"]))
            .await
            .unwrap();
        assert_eq!(selected.kind, expected);
        assert_eq!(
            selected.objects,
            FileSetRequest::new(["opaque.data"]).objects
        );
    }
}

#[tokio::test]
async fn skips_empty_files_rejects_mixtures_and_allows_explicit_trust() {
    let root = tempfile::tempdir().unwrap();
    for (name, bytes) in [
        ("empty.jsonl", " \n"),
        ("plain.jsonl", "{\"id\":1}\n"),
        ("atif.jsonl", "{\"schema_version\":\"ATIF-1.0\"}\n"),
        ("atif.json", "{\n\"schema_version\":\"ATIF-1.0\"\n}\n"),
    ] {
        fs::write(root.path().join(name), bytes).await.unwrap();
    }
    let storage = Arc::new(OpenDalAccessor::new(fs_operator(root.path())));
    let resolver = JsonResolver::default();
    let request = FileSetRequest::new(["empty.jsonl", "plain.jsonl", "plain.jsonl"]);
    let selected = resolver.resolve(storage.clone(), &request).await.unwrap();
    assert_eq!(selected.kind, JsonKind::Records);
    assert_eq!(selected.objects, request.objects[1..]);
    assert_eq!(
        resolver
            .resolve(storage.clone(), &FileSetRequest::new(["empty.jsonl"]))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    for names in [
        ["plain.jsonl", "atif.jsonl"],
        ["atif.jsonl", "plain.jsonl"],
        ["atif.json", "atif.jsonl"],
    ] {
        let error = resolver
            .resolve(storage.clone(), &FileSetRequest::new(names))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        for name in names {
            assert!(error.to_string().contains(name));
        }
    }
    let trusted = resolver.with_sniff_config(SniffConfig {
        policy: SniffPolicy::FirstFile,
        ..Default::default()
    });
    let request = FileSetRequest::new(["empty.jsonl", "plain.jsonl", "missing.jsonl"]);
    assert_eq!(
        trusted.resolve(storage, &request).await.unwrap().objects,
        request.objects[1..]
    );
}

#[tokio::test]
async fn bounds_probe_bytes_and_preserves_storage_errors() {
    let input = Arc::new(
        MemoryAccessor::new(b"{\"id\":1}\ntrailing garbage", 1)
            .with_failure(9, ErrorKind::Unavailable),
    );
    let resolver = JsonResolver::default().with_sniff_config(SniffConfig {
        max_bytes: NonZeroUsize::new(10).unwrap(),
        concurrency: NonZeroUsize::MIN,
        ..Default::default()
    });
    let request = FileSetRequest::new(["a.jsonl"]);
    assert_eq!(
        resolver
            .resolve(input.clone(), &request)
            .await
            .unwrap()
            .kind,
        JsonKind::Records
    );
    let error = resolver
        .with_sniff_config(SniffConfig {
            max_bytes: NonZeroUsize::new(8).unwrap(),
            ..Default::default()
        })
        .resolve(input, &request)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let input =
        Arc::new(MemoryAccessor::new(b"partial", 1).with_failure(2, ErrorKind::Unavailable));
    let error = JsonResolver::default()
        .resolve(input, &request)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert!(error.to_string().contains("a.jsonl"));
}

#[tokio::test]
async fn rejects_explicit_schemas_for_atif() {
    let resolver =
        JsonResolver::default().with_schema_opt(Some(Arc::new(arrow_schema::Schema::empty())));
    let input = Arc::new(MemoryAccessor::new(
        b"{\"schema_version\":\"ATIF-1.0\"}\n",
        1,
    ));
    let error = resolver
        .resolve(input, &FileSetRequest::new(["a.jsonl"]))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn rejects_incomplete_or_invalid_json_with_object_context() {
    for bytes in [b"{".as_slice(), b"not JSON".as_slice()] {
        let input = Arc::new(MemoryAccessor::new(bytes, 1));
        let error = JsonResolver::default()
            .resolve(input, &FileSetRequest::new(["broken.json"]))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(error.to_string().contains("broken.json"));
    }
}

#[cfg(not(feature = "format-atif"))]
#[tokio::test]
async fn recognizes_atif_but_rejects_building_a_disabled_reader() {
    let storage = Arc::new(MemoryAccessor::new(
        b"{\"schema_version\":\"ATIF-1.0\"}\n",
        1,
    ));
    let request = FileSetRequest::new(["a.jsonl"]);
    let resolver = JsonResolver::default();
    let selected = resolver.resolve(storage.clone(), &request).await.unwrap();
    assert_eq!(selected.kind, JsonKind::Atif(AtifFormat::JsonLines));
    let error = resolver
        .reader(storage, selected.kind, selected.objects.first())
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[cfg(feature = "format-atif")]
#[tokio::test]
async fn binds_atif_readers_independently_of_the_ordinary_json_reader() {
    use open_arrow_access_core::ReadRequest;
    use open_arrow_access_test_support::fixture_path;
    let storage = Arc::new(OpenDalAccessor::new(fs_operator(fixture_path!("atif", ""))));
    for (name, expected_rows) in [("trajectory.json", 1), ("trajectories.jsonl", 3)] {
        let resolver = JsonResolver::default();
        let selected = resolver
            .resolve(storage.clone(), &FileSetRequest::new([name]))
            .await
            .unwrap();
        let accessor = resolver
            .reader(storage.clone(), selected.kind, selected.objects.first())
            .unwrap();
        let mut reader = accessor.open(ReadRequest::new(name)).await.unwrap();
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, expected_rows);
    }
}
