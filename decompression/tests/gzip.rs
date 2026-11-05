#![cfg(feature = "gzip")]

mod common;

use std::{error::Error as _, io::Write, num::NonZeroUsize};

use common::{MemoryAccessor, collect};
use flate2::{Compression, write::GzEncoder};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, ErrorKind, ObjectRef,
    Result,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};

fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn members_preserve_order_and_output_chunk_limit() -> Result<()> {
    let mut members = compress(b"first");
    members.extend(compress(b""));
    members.extend(compress("第二行".as_bytes()));
    for (input, expected) in [(members, "first第二行".as_bytes()), (compress(b""), b"")] {
        let accessor = MemoryAccessor::new(&input, 1)
            .layer(DecompressionLayer::new(Gzip).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("data.gz".into()).await?;
        let mut output = Vec::new();
        while let Some(bytes) = reader.next().await? {
            assert!(!bytes.is_empty() && bytes.len() <= 3);
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
    let mut input = MemoryAccessor::new(&compress(b"payload"), 2);
    input.expected_options = Some(options);
    let accessor = input.layer(DecompressionLayer::new(Gzip));
    let mut reader = accessor
        .open(ByteReadRequest::new("data.gz").with_options(options))
        .await
        .unwrap();
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
    let valid = compress(b"payload");
    let mut corrupt = valid.clone();
    let crc = corrupt.len() - 8;
    corrupt[crc] ^= 1;
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"junk");
    let mut truncated_member = valid.clone();
    truncated_member.extend_from_slice(&valid[..valid.len() - 1]);
    for bytes in [
        vec![],
        b"not gzip".to_vec(),
        valid[..3].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        corrupt,
        trailing,
        truncated_member,
    ] {
        let accessor = MemoryAccessor::new(&bytes, 2).layer(DecompressionLayer::new(Gzip));
        let mut reader = accessor.open("bad.gz".into()).await?;
        assert_eq!(
            collect(reader.as_mut()).await.unwrap_err().kind(),
            ErrorKind::Decode
        );
        assert!(reader.next().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn source_errors_are_lazy_and_preserved() -> Result<()> {
    let mut input = MemoryAccessor::new(&[], 1);
    input.error = Some(ErrorKind::PermissionDenied);
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
    let mut bytes = compress(b"ab");
    bytes.extend_from_slice(b"not another member");
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
    let accessor = MemoryAccessor::new(&compress(&compress(b"nested")), 2)
        .layer(DecompressionLayer::new(Gzip))
        .layer(DecompressionLayer::new(Gzip));
    let mut first = accessor.open("nested.gz.gz".into()).await?;
    let mut second = accessor.open("nested.gz.gz".into()).await?;
    assert_eq!(collect(first.as_mut()).await?, b"nested");
    assert_eq!(collect(second.as_mut()).await?, b"nested");
    Ok(())
}
