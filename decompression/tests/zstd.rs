use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{DecompressionLayer, zstd::Zstd};

use open_arrow_access_test_support::{MemoryAccessor, byte_layer, fixture};

#[tokio::test]
async fn concatenated_empty_and_skippable_frames_preserve_order_with_bounded_output() -> Result<()>
{
    let bytes = fixture!("zstd", "good_frames.zst").await;
    for chunk_size in [1, 7, 4096] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Zstd).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.zst".into()).await?;
        byte_layer::assert_output(reader.as_mut(), "first\n第二行\n".as_bytes(), 3).await?;
    }
    Ok(())
}

#[tokio::test]
async fn empty_frames_are_valid_but_empty_input_is_not() -> Result<()> {
    for name in ["empty_frame.zst", "skippable_frame.zst"] {
        let bytes = fixture!("zstd", name).await;
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Zstd));
        let mut reader = accessor.open("empty.zst".into()).await?;
        assert!(reader.next().await?.is_none());
        assert!(reader.next().await?.is_none());
    }
    let input = MemoryAccessor::new(&fixture!("zstd", "bad_empty.zst").await, 1);
    let observed = input.observations.clone();
    let accessor = input.layer(DecompressionLayer::new(Zstd));
    let mut reader = accessor.open("missing-header.zst".into()).await?;
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
        let bytes = fixture!("zstd", name).await;
        let memory = MemoryAccessor::new(&bytes, 1);
        let observed = memory.observations.clone();
        let mut reader = memory
            .layer(DecompressionLayer::new(Zstd))
            .open("invalid.zst".into())
            .await
            .unwrap();
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
        |input| input.layer(DecompressionLayer::new(Zstd)),
        &fixture!("zstd", "payload.zst").await,
        b"payload",
        "decompress",
    )
    .await
}
