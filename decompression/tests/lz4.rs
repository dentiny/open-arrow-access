#![cfg(feature = "lz4")]

mod common;

use std::{io::Write, num::NonZeroUsize, sync::atomic::Ordering};

use lz4_flex::frame::{FrameEncoder, FrameInfo};
use open_arrow_access_core::{Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, ErrorKind};
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
async fn concatenated_frames_and_skippable_frames_preserve_order_across_fragments() {
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
        let mut reader = accessor.open("records.lz4".into()).await.unwrap();
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            assert!(!chunk.is_empty() && chunk.len() <= 3);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, "first\n第二行\n".as_bytes());
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn valid_empty_or_skippable_frames_are_eof_but_empty_input_is_invalid() {
    for bytes in [compress(b""), skippable(b"metadata")] {
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("empty.lz4".into()).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
    }
    let accessor = MemoryAccessor::new(&[], 1).layer(DecompressionLayer::new(Lz4));
    let mut reader = accessor.open("missing.lz4".into()).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
}

#[tokio::test]
async fn corrupt_truncated_raw_legacy_and_trailing_input_fail_terminally() {
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
    for (case, bytes) in cases.into_iter().enumerate() {
        let input = MemoryAccessor::new(&bytes, 1);
        let polls = input.polls.clone();
        let accessor = input.layer(DecompressionLayer::new(Lz4));
        let mut reader = accessor.open("bad.lz4".into()).await.unwrap();
        let result = collect(reader.as_mut()).await;
        assert!(result.is_err(), "accepted case {case}: {result:?}");
        assert_eq!(result.unwrap_err().kind(), ErrorKind::Decode);
        let before = polls.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), before);
    }
}

#[tokio::test]
async fn layer_is_lazy_forwards_options_and_preserves_source_errors() {
    let mut input = MemoryAccessor::new(&compress(&vec![b'x'; 100_000]), 1);
    input.error = Some(ErrorKind::PermissionDenied);
    let polls = input.polls.clone();
    let requests = input.requests.clone();
    let accessor = input.layer(DecompressionLayer::new(Lz4));
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    let mut reader = accessor
        .open(ByteReadRequest::new("input.lz4").with_options(options))
        .await
        .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(requests.lock().unwrap()[0].options, options);
    assert!(!accessor.capability().read_with_range);
    assert!(!reader.capability().read_with_range);
    assert_eq!(reader.metadata().content_length, None);
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
}
