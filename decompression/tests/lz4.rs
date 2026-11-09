use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, AccessorExt, Result};
use open_arrow_access_decompression::{DecompressionLayer, lz4::Lz4};

use open_arrow_access_test_support::{MemoryAccessor, byte_layer, fixture};

#[tokio::test]
async fn concatenated_frames_and_skippable_frames_preserve_order_across_fragments() -> Result<()> {
    let bytes = fixture!("lz4", "good_frames.lz4").await;
    byte_layer::assert_fragmented_output(
        |input| {
            input.layer(DecompressionLayer::new(Lz4).with_chunk_size(NonZeroUsize::new(3).unwrap()))
        },
        &bytes,
        "first\n第二行\n".as_bytes(),
        &[1, 7, bytes.len()],
        3,
    )
    .await?;
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
    byte_layer::assert_decode_error(
        |input| input.layer(DecompressionLayer::new(Lz4)),
        &fixture!("lz4", "bad_empty.lz4").await,
        "decompress",
    )
    .await?;
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
        byte_layer::assert_decode_error(
            |input| input.layer(DecompressionLayer::new(Lz4)),
            &fixture!("lz4", name).await,
            "decompress",
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn error_codes_sources_and_terminal_state_follow_the_byte_contract() -> Result<()> {
    byte_layer::assert_error_contract(
        |input| input.layer(DecompressionLayer::new(Lz4)),
        &fixture!("lz4", "payload.lz4").await,
        "decompress",
    )
    .await
}
