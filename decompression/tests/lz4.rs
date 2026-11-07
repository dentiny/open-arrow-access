use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{DecompressionLayer, Lz4};

use open_arrow_access_test_support::{MemoryAccessor, byte_layer, fixture};

#[tokio::test]
async fn concatenated_frames_and_skippable_frames_preserve_order_across_fragments() -> Result<()> {
    let bytes = fixture!("lz4", "good_frames.lz4").await;
    for input_chunk in [1, 7, bytes.len()] {
        let input = MemoryAccessor::new(&bytes, input_chunk);
        let accessor = input
            .layer(DecompressionLayer::new(Lz4).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.lz4".into()).await?;
        byte_layer::assert_output(reader.as_mut(), "first\n第二行\n".as_bytes(), 3).await?;
    }
    Ok(())
}

#[tokio::test]
async fn valid_empty_or_skippable_frames_are_eof_but_empty_input_is_invalid() -> Result<()> {
    for name in ["empty_frame.lz4", "skippable_frame.lz4"] {
        let bytes = fixture!("lz4", name).await;
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("empty.lz4".into()).await?;
        assert!(reader.next().await?.is_none());
    }
    let input = MemoryAccessor::new(&fixture!("lz4", "bad_empty.lz4").await, 1);
    let observed = input.observations.clone();
    let accessor = input.layer(DecompressionLayer::new(Lz4));
    let mut reader = accessor.open("missing.lz4".into()).await?;
    byte_layer::assert_terminal_error(
        reader.as_mut(),
        &observed.reads,
        ErrorKind::Decode,
        "decompress",
    )
    .await;
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
        let bytes = fixture!("lz4", name).await;
        let input = MemoryAccessor::new(&bytes, 1);
        let observed = input.observations.clone();
        let accessor = input.layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("bad.lz4".into()).await?;
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
async fn error_codes_sources_and_terminal_state_follow_the_byte_contract() -> Result<()> {
    byte_layer::assert_error_contract(
        |input| input.layer(DecompressionLayer::new(Lz4)),
        &fixture!("lz4", "payload.lz4").await,
        b"payload",
        "decompress",
    )
    .await
}
