#![cfg(feature = "lz4")]

mod common;

use std::{io::Write, num::NonZeroUsize};

use lz4_flex::frame::{FrameEncoder, FrameInfo};
use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, Result};
use open_arrow_access_decompression::{DecompressionLayer, Lz4};

use common::{MemoryAccessor, collect};

fn compress(input: &[u8]) -> Vec<u8> {
    let info = FrameInfo::new()
        .block_checksums(true)
        .content_checksum(true);
    let mut encoder = FrameEncoder::with_frame_info(info, Vec::new());
    encoder.write_all(input).unwrap();
    encoder.finish().unwrap()
}

fn skippable(payload: &[u8]) -> Vec<u8> {
    let mut output = 0x184d2a50u32.to_le_bytes().to_vec();
    output.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    output.extend_from_slice(payload);
    output
}

#[tokio::test]
async fn concatenated_frames_and_skippable_frames_preserve_order_across_fragments() -> Result<()> {
    let mut bytes = skippable(b"before");
    bytes.extend(compress(b"first\n"));
    bytes.extend(skippable(b"between"));
    bytes.extend(compress(b""));
    bytes.extend(compress("第二行\n".as_bytes()));
    bytes.extend(skippable(b"after"));
    for input_chunk in [1, 7, bytes.len()] {
        let input = MemoryAccessor::new(&bytes, input_chunk);
        let accessor = input
            .layer(DecompressionLayer::new(Lz4).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.lz4".into()).await?;
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
async fn valid_empty_or_skippable_frames_are_eof_but_empty_input_is_invalid() -> Result<()> {
    for bytes in [compress(b""), skippable(b"metadata")] {
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("empty.lz4".into()).await?;
        assert!(reader.next().await?.is_none());
    }
    let accessor = MemoryAccessor::new(&[], 1).layer(DecompressionLayer::new(Lz4));
    let mut reader = accessor.open("missing.lz4".into()).await?;
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    Ok(())
}

#[tokio::test]
async fn corrupt_truncated_raw_legacy_and_trailing_input_fail_terminally() -> Result<()> {
    let valid = compress(b"payload");
    let mut checksum = valid.clone();
    *checksum.last_mut().unwrap() ^= 1;
    let mut header = valid.clone();
    header[6] ^= 1;
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"trailing junk");
    let mut truncated_next = valid.clone();
    truncated_next.extend_from_slice(&valid[..valid.len() - 1]);
    let block = lz4_flex::block::compress(b"payload");
    let mut legacy = 0x184c2102u32.to_le_bytes().to_vec();
    legacy.extend_from_slice(&(block.len() as u32).to_le_bytes());
    legacy.extend(block);
    let cases = [
        valid[..3].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        checksum,
        header,
        trailing,
        truncated_next,
        lz4_flex::block::compress_prepend_size(b"payload"),
        legacy,
        skippable(b"metadata")[..10].to_vec(),
    ];
    for bytes in cases {
        let input = MemoryAccessor::new(&bytes, 1);
        let accessor = input.layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("bad.lz4".into()).await?;
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
    let accessor = input.layer(DecompressionLayer::new(Lz4));
    let mut reader = accessor.open("source.lz4".into()).await?;
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}
