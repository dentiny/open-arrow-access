mod common;

use std::num::NonZeroUsize;

use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{DecompressionLayer, Lz4};

use common::{MemoryAccessor, collect, fixture};

#[tokio::test]
async fn concatenated_frames_and_skippable_frames_preserve_order_across_fragments() -> Result<()> {
    let bytes = fixture("lz4", "good_frames.lz4");
    for input_chunk in [1, 7, bytes.len()] {
        let input = MemoryAccessor::new(&bytes, input_chunk);
        let accessor = input
            .layer(DecompressionLayer::new(Lz4).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.lz4".into()).await?;
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
async fn valid_empty_or_skippable_frames_are_eof_but_empty_input_is_invalid() -> Result<()> {
    for name in ["empty_frame.lz4", "skippable_frame.lz4"] {
        let bytes = fixture("lz4", name);
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("empty.lz4".into()).await?;
        assert!(reader.next().await?.is_none());
    }
    let input = MemoryAccessor::new(&fixture("lz4", "bad_empty.lz4"), 1);
    let reads = input.reads.clone();
    let accessor = input.layer(DecompressionLayer::new(Lz4));
    let mut reader = accessor.open("missing.lz4".into()).await?;
    common::assert_terminal_error(reader.as_mut(), &reads, ErrorKind::Decode).await;
    Ok(())
}

#[tokio::test]
async fn corrupt_truncated_raw_legacy_and_trailing_input_fail_terminally() -> Result<()> {
    for name in [
        "bad_header.lz4",
        "bad_truncated.lz4",
        "bad_checksum.lz4",
        "bad_header_checksum.lz4",
        "bad_trailing.lz4",
        "bad_truncated_second.lz4",
        "bad_raw_block.lz4",
        "bad_legacy.lz4",
        "bad_truncated_skippable.lz4",
    ] {
        let bytes = fixture("lz4", name);
        let input = MemoryAccessor::new(&bytes, 1);
        let reads = input.reads.clone();
        let accessor = input.layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("bad.lz4".into()).await?;
        common::assert_terminal_error(reader.as_mut(), &reads, ErrorKind::Decode).await;
    }
    Ok(())
}

#[tokio::test]
async fn source_errors_are_preserved() -> Result<()> {
    let mut input = MemoryAccessor::new(&fixture("lz4", "payload.lz4"), 2);
    input.error = Some(ErrorKind::PermissionDenied);
    let accessor = input.layer(DecompressionLayer::new(Lz4));
    let mut reader = accessor.open("source.lz4".into()).await?;
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn error_codes_sources_and_terminal_state_follow_the_byte_contract() -> Result<()> {
    common::error_contract(Lz4, "lz4", "payload.lz4").await
}
