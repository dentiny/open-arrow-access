use open_arrow_access_core::{Accessor, AccessorExt, ByteReadRequest, ErrorKind};
use open_arrow_access_decompression::{AutoDecompressionLayer, Compression};
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes};

#[test]
fn recognizes_only_complete_case_sensitive_compression_suffixes() {
    for (name, expected) in [
        #[cfg(feature = "gzip")]
        ("data.jsonl.gz", Compression::Gzip),
        #[cfg(feature = "gzip")]
        ("data.tgz", Compression::Gzip),
        #[cfg(feature = "bz2")]
        ("data.bz2", Compression::Bzip2),
        #[cfg(feature = "lz4")]
        ("data.lz4", Compression::Lz4),
        #[cfg(feature = "zstd")]
        ("data.zst", Compression::Zstd),
        #[cfg(feature = "zstd")]
        ("data.zstd", Compression::Zstd),
        ("data.tar", Compression::Uncompressed),
        ("data.zip", Compression::Uncompressed),
        ("data.GZ", Compression::Uncompressed),
        ("data.gz.backup", Compression::Uncompressed),
        ("data.gz?name=plain", Compression::Uncompressed),
        ("dir.gz/file", Compression::Uncompressed),
    ] {
        assert_eq!(Compression::from_path(name).unwrap(), expected);
    }
}

#[tokio::test]
async fn plain_objects_keep_ranges_metadata_and_storage_options() {
    use futures::TryStreamExt;
    use open_arrow_access_core::{ByteAccessor, ByteReadOptions, ListRequest};
    use std::num::NonZeroUsize;
    let input = MemoryAccessor::new(b"plain", 2)
        .with_ranges()
        .with_listing()
        .with_version("v1");
    let observations = input.observations.clone();
    let accessor = input.layer(AutoDecompressionLayer::new());
    assert!(!accessor.capability().read_with_range);
    assert!(accessor.capability().list);
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: NonZeroUsize::new(3),
    };
    let mut reader = accessor
        .open(ByteReadRequest::new("data.json").with_options(options))
        .await
        .unwrap();
    assert!(reader.capability().read_with_range);
    assert_eq!(reader.read_range(1..4).await.unwrap().as_ref(), b"lai");
    assert_eq!(reader.metadata().content_length, Some(5));
    assert_eq!(reader.metadata().version.as_deref(), Some("v1"));
    assert_eq!(observations.requests.lock().unwrap()[0].options, options);
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"plain");
    let listed: Vec<_> = accessor
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!listed.is_empty());
}

#[tokio::test]
async fn disabled_codec_is_rejected_before_storage_opens() {
    for (name, enabled) in [
        ("x.gz", cfg!(feature = "gzip")),
        ("x.bz2", cfg!(feature = "bz2")),
        ("x.lz4", cfg!(feature = "lz4")),
        ("x.zst", cfg!(feature = "zstd")),
    ] {
        if enabled {
            continue;
        }
        let error = Compression::from_path(name).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        let input = MemoryAccessor::new(b"anything", 1);
        let observations = input.observations.clone();
        let accessor = input.layer(AutoDecompressionLayer::new());
        let error = accessor.open(name.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(observations.requests.lock().unwrap().is_empty());
    }
}

#[cfg(feature = "gzip")]
#[tokio::test]
async fn gzip_chunks_and_corruption_use_the_existing_decoder() {
    use open_arrow_access_test_support::fixture;
    use std::num::NonZeroUsize;
    for size in [1, 7, 4096] {
        let accessor = MemoryAccessor::new(&fixture!("gzip", "payload.gz").await, size)
            .layer(AutoDecompressionLayer::new().with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("input.gz".into()).await.unwrap();
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            assert!(chunk.len() <= 3);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, b"payload");
    }
    let accessor = MemoryAccessor::new(&fixture!("gzip", "bad_truncated.gz").await, 1)
        .layer(AutoDecompressionLayer::new());
    let mut reader = accessor.open("input.gz".into()).await.unwrap();
    loop {
        match reader.next().await {
            Ok(Some(_)) => {}
            Err(error) => {
                assert_eq!(error.kind(), ErrorKind::Decode);
                break;
            }
            Ok(None) => panic!("truncated gzip accepted"),
        }
    }
    assert!(reader.next().await.unwrap().is_none());
}

// Each enabled codec is exercised independently in single-feature builds too.
macro_rules! codec_route_test {
    ($test:ident, $feature:literal, $directory:literal, $fixture:literal, $name:literal) => {
        #[cfg(feature = $feature)]
        #[tokio::test]
        async fn $test() {
            use open_arrow_access_test_support::fixture;
            let bytes = fixture!($directory, $fixture).await;
            let accessor = MemoryAccessor::new(&bytes, 1).layer(AutoDecompressionLayer::new());
            let mut reader = accessor.open($name.into()).await.unwrap();
            assert!(!reader.capability().read_with_range);
            assert_eq!(reader.metadata().content_length, None);
            assert_eq!(reader.metadata().source.as_ref().unwrap().locator(), $name);
            assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"payload");
        }
    };
}
codec_route_test!(routes_gzip, "gzip", "gzip", "payload.gz", "data.gz");
codec_route_test!(routes_bzip2, "bz2", "bz2", "payload.bz2", "data.bz2");
codec_route_test!(routes_lz4, "lz4", "lz4", "payload.lz4", "data.lz4");
codec_route_test!(routes_zstd, "zstd", "zstd", "payload.zst", "data.zstd");
