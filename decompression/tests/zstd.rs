#![cfg(feature = "zstd")]

mod common;

use std::{io::Write, num::NonZeroUsize};

use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{DecompressionLayer, Zstd};

use common::{MemoryAccessor, collect};

fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
    encoder.include_checksum(true).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn skippable(bytes: &[u8]) -> Vec<u8> {
    let mut frame = 0x184D2A50_u32.to_le_bytes().to_vec();
    frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    frame.extend_from_slice(bytes);
    frame
}

#[tokio::test]
async fn concatenated_empty_and_skippable_frames_preserve_order_with_bounded_output() -> Result<()>
{
    let mut bytes = skippable(b"metadata");
    bytes.extend(compress(b"first\n"));
    bytes.extend(compress(b""));
    bytes.extend(skippable(b"ignored"));
    bytes.extend(compress("第二行\n".as_bytes()));
    bytes.extend(skippable(b""));
    for chunk_size in [1, 7, 4096] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Zstd).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.zst".into()).await?;
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await? {
            assert!(!chunk.is_empty() && chunk.len() <= 3);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, "first\n第二行\n".as_bytes());
        assert!(reader.next().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn empty_frames_are_valid_but_empty_input_is_not() -> Result<()> {
    for bytes in [compress(b""), skippable(b"metadata")] {
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Zstd));
        let mut reader = accessor.open("empty.zst".into()).await?;
        assert!(reader.next().await?.is_none());
        assert!(reader.next().await?.is_none());
    }
    let accessor = MemoryAccessor::new(&[], 1).layer(DecompressionLayer::new(Zstd));
    let mut reader = accessor.open("missing-header.zst".into()).await?;
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn corruption_truncation_and_trailing_junk_are_terminal_errors() -> Result<()> {
    let valid = compress(b"payload");
    let mut corrupt_checksum = valid.clone();
    *corrupt_checksum.last_mut().unwrap() ^= 1;
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"trailing junk");
    let mut truncated_second = valid.clone();
    truncated_second.extend_from_slice(&valid[..valid.len() - 1]);
    let skipped = skippable(b"metadata");
    let cases = [
        b"not a zstd frame".to_vec(),
        valid[..3].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        corrupt_checksum,
        trailing,
        truncated_second,
        skipped[..skipped.len() - 1].to_vec(),
    ];
    for bytes in cases {
        let memory = MemoryAccessor::new(&bytes, 1);
        let mut reader = memory
            .layer(DecompressionLayer::new(Zstd))
            .open("invalid.zst".into())
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
    let mut input = MemoryAccessor::new(&compress(b"payload"), 2);
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
