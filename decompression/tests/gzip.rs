use std::{error::Error as _, num::NonZeroUsize};

use futures::TryStreamExt;
use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, ErrorKind, ObjectRef,
    Result,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use open_arrow_access_test_support::{
    MemoryAccessor, byte_layer, collect_bytes as collect, fixture,
};

#[tokio::test]
async fn members_preserve_order_and_output_chunk_limit() -> Result<()> {
    for (name, expected) in [
        ("good_multi_member.gz", "first第二行".as_bytes()),
        ("empty_member.gz", &b""[..]),
    ] {
        let input = fixture("gzip", name);
        let accessor = MemoryAccessor::new(&input, 1)
            .layer(DecompressionLayer::new(Gzip).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("data.gz".into()).await?;
        let mut output = Vec::new();
        while let Some(bytes) = reader.next().await? {
            assert_gt!(bytes.len(), 0);
            assert_le!(bytes.len(), 3);
            output.extend_from_slice(&bytes);
        }
        assert_eq!(output, expected);
        assert!(reader.next().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn layer_preserves_metadata_options_and_listing_but_removes_ranges() -> Result<()> {
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(4).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    let input = MemoryAccessor::new(&fixture("gzip", "payload.gz"), 2)
        .with_ranges()
        .with_listing()
        .with_version("source-version");
    let observed = input.observations.clone();
    let accessor = input.layer(DecompressionLayer::new(Gzip));
    let mut reader = accessor
        .open(ByteReadRequest::new("data.gz").with_options(options))
        .await
        .unwrap();
    assert_eq!(observed.requests.lock().unwrap()[0].options, options);
    let capability = accessor.capability();
    assert!(capability.read && capability.list && !capability.read_with_range);
    assert!(!reader.capability().list && !reader.capability().read_with_range);
    let metadata = reader.metadata();
    assert_eq!(metadata.content_length, None);
    assert_eq!(metadata.source.as_ref().unwrap().locator(), "data.gz");
    assert_eq!(metadata.version.as_deref(), Some("source-version"));
    assert_eq!(
        reader.read_range(0..1).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(collect(reader.as_mut()).await?, b"payload");
    let objects = accessor
        .list("prefix".into())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(objects, [ObjectRef::new("prefix")]);
    Ok(())
}

#[tokio::test]
async fn invalid_gzip_is_terminal() -> Result<()> {
    for name in [
        "bad_empty.gz",
        "bad_magic.gz",
        "bad_header.gz",
        "bad_truncated.gz",
        "bad_checksum.gz",
        "bad_trailing.gz",
        "bad_truncated_member.gz",
    ] {
        let bytes = fixture("gzip", name);
        let input = MemoryAccessor::new(&bytes, 2);
        let observed = input.observations.clone();
        let accessor = input.layer(DecompressionLayer::new(Gzip));
        let mut reader = accessor.open("bad.gz".into()).await?;
        byte_layer::assert_terminal_error(
            reader.as_mut(),
            &observed.reads,
            ErrorKind::Decode,
            "decompress",
        )
        .await;
    }
    Ok(())
}

#[tokio::test]
async fn source_errors_are_lazy_and_preserved() -> Result<()> {
    let input = MemoryAccessor::new(&[], 1).with_failure(0, ErrorKind::PermissionDenied);
    let accessor = input.layer(DecompressionLayer::new(Gzip));
    let mut reader = accessor.open("source.gz".into()).await?;
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert!(
        error
            .source()
            .unwrap()
            .to_string()
            .contains("injected source failure")
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn reading_does_not_decode_the_whole_input() -> Result<()> {
    let bytes = fixture("gzip", "bad_late_member.gz");
    let accessor = MemoryAccessor::new(&bytes, 1)
        .layer(DecompressionLayer::new(Gzip).with_chunk_size(NonZeroUsize::MIN));
    let mut reader = accessor.open("partial.gz".into()).await?;
    assert_eq!(reader.next().await?.unwrap().as_ref(), b"a");
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    Ok(())
}

#[tokio::test]
async fn nested_layers_and_readers_have_independent_state() -> Result<()> {
    let accessor = MemoryAccessor::new(&fixture("gzip", "nested.gz.gz"), 2)
        .layer(DecompressionLayer::new(Gzip))
        .layer(DecompressionLayer::new(Gzip));
    let mut first = accessor.open("nested.gz.gz".into()).await?;
    let mut second = accessor.open("nested.gz.gz".into()).await?;
    assert_eq!(collect(first.as_mut()).await?, b"nested");
    assert_eq!(collect(second.as_mut()).await?, b"nested");
    Ok(())
}

#[tokio::test]
async fn error_codes_sources_and_terminal_state_follow_the_byte_contract() -> Result<()> {
    byte_layer::assert_error_contract(
        |input| input.layer(DecompressionLayer::new(Gzip)),
        &fixture("gzip", "payload.gz"),
        b"payload",
        "decompress",
    )
    .await
}
