use open_arrow_access_core::{Accessor, AccessorExt, ByteReadRequest, ErrorKind};
use open_arrow_access_decompression::{AutoDecompressionLayer, Compression};
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes};

#[test]
fn recognizes_complete_compression_suffixes_ignoring_ascii_case() {
    for (name, expected) in [
        #[cfg(feature = "gzip")]
        ("data.jsonl.gz", Compression::Gzip),
        #[cfg(feature = "gzip")]
        ("目录/data.JSONL.GZ", Compression::Gzip),
        #[cfg(feature = "gzip")]
        ("data.Gz", Compression::Gzip),
        #[cfg(feature = "gzip")]
        ("data.tgz", Compression::Gzip),
        #[cfg(feature = "gzip")]
        ("目录/data.JSONL.TGZ", Compression::Gzip),
        #[cfg(feature = "gzip")]
        ("data.Tgz", Compression::Gzip),
        #[cfg(feature = "bz2")]
        ("data.bz2", Compression::Bzip2),
        #[cfg(feature = "bz2")]
        ("目录/data.JSONL.BZ2", Compression::Bzip2),
        #[cfg(feature = "bz2")]
        ("data.Bz2", Compression::Bzip2),
        #[cfg(feature = "lz4")]
        ("data.lz4", Compression::Lz4),
        #[cfg(feature = "lz4")]
        ("目录/data.JSONL.LZ4", Compression::Lz4),
        #[cfg(feature = "lz4")]
        ("data.Lz4", Compression::Lz4),
        #[cfg(feature = "zstd")]
        ("data.zst", Compression::Zstd),
        #[cfg(feature = "zstd")]
        ("目录/data.JSONL.ZST", Compression::Zstd),
        #[cfg(feature = "zstd")]
        ("data.Zst", Compression::Zstd),
        #[cfg(feature = "zstd")]
        ("data.zstd", Compression::Zstd),
        #[cfg(feature = "zstd")]
        ("目录/data.JSONL.ZSTD", Compression::Zstd),
        #[cfg(feature = "zstd")]
        ("data.Zstd", Compression::Zstd),
        ("data", Compression::Uncompressed),
        ("data.GZ.backup", Compression::Uncompressed),
        ("data.GZ?name=plain", Compression::Uncompressed),
        ("data.GZ#fragment", Compression::Uncompressed),
        ("dir.GZ/file", Compression::Uncompressed),
        ("data.tar", Compression::Uncompressed),
        ("data.zip", Compression::Uncompressed),
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
        ("x.GZ", cfg!(feature = "gzip")),
        ("x.TgZ", cfg!(feature = "gzip")),
        ("x.bz2", cfg!(feature = "bz2")),
        ("x.Bz2", cfg!(feature = "bz2")),
        ("x.lz4", cfg!(feature = "lz4")),
        ("x.LZ4", cfg!(feature = "lz4")),
        ("x.zst", cfg!(feature = "zstd")),
        ("x.ZST", cfg!(feature = "zstd")),
        ("x.ZsTd", cfg!(feature = "zstd")),
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
async fn routes_gzip_with_chunk_limit_metadata_and_terminal_errors() {
    use more_asserts::assert_le;
    use open_arrow_access_test_support::{fixture, reader};
    use std::num::NonZeroUsize;

    // Fragmentation matrices belong to the codec suite; this checks the auto
    // route and its chunk-size option with one representative fragmented input.
    let accessor = MemoryAccessor::new(&fixture!("gzip", "payload.gz").await, 1)
        .layer(AutoDecompressionLayer::new().with_chunk_size(NonZeroUsize::new(3).unwrap()));
    let mut reader = accessor.open("input.gz".into()).await.unwrap();
    assert!(!reader.capability().read_with_range);
    assert_eq!(reader.metadata().content_length, None);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "input.gz"
    );
    let mut output = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        assert_le!(chunk.len(), 3);
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, b"payload");
    let accessor = MemoryAccessor::new(&fixture!("gzip", "bad_truncated.gz").await, 1)
        .layer(AutoDecompressionLayer::new());
    let mut reader = accessor.open("input.gz".into()).await.unwrap();
    assert_eq!(
        reader::next_error(reader.as_mut()).await.kind(),
        ErrorKind::Decode
    );
    assert!(reader.next().await.unwrap().is_none());
}

// Each enabled codec is exercised independently in single-feature builds too.
macro_rules! codec_route_test {
    ($test:ident, $feature:literal, $directory:literal, $fixture:literal, $($name:literal),+) => {
        #[cfg(feature = $feature)]
        #[tokio::test]
        async fn $test() {
            use open_arrow_access_test_support::fixture;
            let bytes = fixture!($directory, $fixture).await;
            for name in [$($name),+] {
                let input = MemoryAccessor::new(&bytes, 1);
                let observations = input.observations.clone();
                let accessor = input.layer(AutoDecompressionLayer::new());
                let mut reader = accessor.open(name.into()).await.unwrap();
                assert!(!reader.capability().read_with_range);
                assert_eq!(reader.metadata().content_length, None);
                assert_eq!(reader.metadata().source.as_ref().unwrap().locator(), name);
                assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"payload");
                assert_eq!(observations.requests.lock().unwrap()[0].object.locator(), name);
            }
        }
    };
}
codec_route_test!(
    routes_gzip_suffixes,
    "gzip",
    "gzip",
    "payload.gz",
    "data.gz",
    "data.JSONL.GZ",
    "data.Gz",
    "data.TGZ",
    "data.TgZ"
);
codec_route_test!(
    routes_bzip2,
    "bz2",
    "bz2",
    "payload.bz2",
    "data.bz2",
    "data.BZ2",
    "data.Bz2"
);
codec_route_test!(
    routes_lz4,
    "lz4",
    "lz4",
    "payload.lz4",
    "data.lz4",
    "data.LZ4",
    "data.Lz4"
);
codec_route_test!(
    routes_zstd,
    "zstd",
    "zstd",
    "payload.zst",
    "data.zstd",
    "data.ZSTD",
    "data.ZsTd",
    "data.ZST",
    "data.Zst"
);
