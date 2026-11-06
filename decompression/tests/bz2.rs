mod common;

use std::num::NonZeroUsize;

use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{Bzip2, DecompressionLayer};

use common::{MemoryAccessor, collect, fixture};

#[tokio::test]
async fn concatenated_members_preserve_order_with_fragmented_input_and_bounded_output() -> Result<()>
{
    let bytes = fixture("bz2", "good_members.bz2");
    for chunk_size in [1, 7, bytes.len()] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Bzip2).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.bz2".into()).await?;
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await? {
            assert_gt!(chunk.len(), 0);
            assert_le!(chunk.len(), 3);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, "first\n第二行\n".as_bytes());
        assert!(reader.next().await?.is_none());
    }
    let accessor = MemoryAccessor::new(&fixture("bz2", "empty_member.bz2"), 1)
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
        let bytes = fixture("bz2", name);
        let input = MemoryAccessor::new(&bytes, 1);
        let mut reader = input
            .layer(DecompressionLayer::new(Bzip2))
            .open("bad.bz2".into())
            .await
            .unwrap();
        assert_eq!(
            collect(reader.as_mut()).await.unwrap_err().kind(),
            ErrorKind::Decode
        );
        assert!(reader.next().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn source_errors_are_preserved() -> Result<()> {
    let mut input = MemoryAccessor::new(&fixture("bz2", "payload.bz2"), 2);
    input.error = Some(ErrorKind::PermissionDenied);
    let accessor = input.layer(DecompressionLayer::new(Bzip2));
    let mut reader = accessor.open("source.bz2".into()).await?;
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}
