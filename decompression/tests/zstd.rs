mod common;

use std::num::NonZeroUsize;

use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{DecompressionLayer, Zstd};

use common::{MemoryAccessor, collect, fixture};

#[tokio::test]
async fn concatenated_empty_and_skippable_frames_preserve_order_with_bounded_output() -> Result<()>
{
    let bytes = fixture("zstd", "good_frames.zst");
    for chunk_size in [1, 7, 4096] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Zstd).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.zst".into()).await?;
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await? {
            assert_gt!(chunk.len(), 0);
            assert_le!(chunk.len(), 3);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, "first\n第二行\n".as_bytes());
        assert!(reader.next().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn empty_frames_are_valid_but_empty_input_is_not() -> Result<()> {
    for name in ["empty_frame.zst", "skippable_frame.zst"] {
        let bytes = fixture("zstd", name);
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Zstd));
        let mut reader = accessor.open("empty.zst".into()).await?;
        assert!(reader.next().await?.is_none());
        assert!(reader.next().await?.is_none());
    }
    let input = MemoryAccessor::new(&fixture("zstd", "bad_empty.zst"), 1);
    let reads = input.reads.clone();
    let accessor = input.layer(DecompressionLayer::new(Zstd));
    let mut reader = accessor.open("missing-header.zst".into()).await?;
    common::assert_terminal_error(reader.as_mut(), &reads, ErrorKind::Decode).await;
    Ok(())
}

#[tokio::test]
async fn corruption_truncation_and_trailing_junk_are_terminal_errors() -> Result<()> {
    for name in [
        "bad_magic.zst",
        "bad_header.zst",
        "bad_truncated.zst",
        "bad_checksum.zst",
        "bad_trailing.zst",
        "bad_truncated_second.zst",
        "bad_truncated_skippable.zst",
    ] {
        let bytes = fixture("zstd", name);
        let memory = MemoryAccessor::new(&bytes, 1);
        let reads = memory.reads.clone();
        let mut reader = memory
            .layer(DecompressionLayer::new(Zstd))
            .open("invalid.zst".into())
            .await
            .unwrap();
        common::assert_terminal_error(reader.as_mut(), &reads, ErrorKind::Decode).await;
    }
    Ok(())
}

#[tokio::test]
async fn source_errors_are_preserved() -> Result<()> {
    let mut input = MemoryAccessor::new(&fixture("zstd", "payload.zst"), 2);
    input.error = Some(ErrorKind::PermissionDenied);
    let accessor = input.layer(DecompressionLayer::new(Zstd));
    let mut reader = accessor.open("source.zstd".into()).await?;
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn error_codes_sources_and_terminal_state_follow_the_byte_contract() -> Result<()> {
    common::error_contract(Zstd, "zstd", "payload.zst").await
}
