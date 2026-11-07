use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{Bzip2, DecompressionLayer};

use open_arrow_access_test_support::{MemoryAccessor, byte_layer, fixture};

#[tokio::test]
async fn concatenated_members_preserve_order_with_fragmented_input_and_bounded_output() -> Result<()>
{
    let bytes = fixture!("bz2", "good_members.bz2").await;
    for chunk_size in [1, 7, bytes.len()] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Bzip2).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.bz2".into()).await?;
        byte_layer::assert_output(reader.as_mut(), "first\n第二行\n".as_bytes(), 3).await?;
    }
    let accessor = MemoryAccessor::new(&fixture!("bz2", "empty_member.bz2").await, 1)
        .layer(DecompressionLayer::new(Bzip2));
    let mut reader = accessor.open("empty.bz2".into()).await?;
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn invalid_truncated_checksum_and_trailing_input_fail_terminally() -> Result<()> {
    for name in [
        "bad_empty.bz2",
        "bad_magic.bz2",
        "bad_header.bz2",
        "bad_truncated.bz2",
        "bad_block_checksum.bz2",
        "bad_stream_checksum.bz2",
        "bad_trailing.bz2",
        "bad_truncated_member.bz2",
    ] {
        let bytes = fixture!("bz2", name).await;
        let input = MemoryAccessor::new(&bytes, 1);
        let observed = input.observations.clone();
        let mut reader = input
            .layer(DecompressionLayer::new(Bzip2))
            .open("bad.bz2".into())
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
        |input| input.layer(DecompressionLayer::new(Bzip2)),
        &fixture!("bz2", "payload.bz2").await,
        b"payload",
        "decompress",
    )
    .await
}
