#![cfg(feature = "zstd")]

mod common;

use std::{io::Write, num::NonZeroUsize, sync::atomic::Ordering};

use open_arrow_access_core::{Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, ErrorKind};
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
async fn concatenated_empty_and_skippable_frames_preserve_order_with_bounded_output() {
    let mut bytes = skippable(b"metadata");
    bytes.extend(compress(b"first\n"));
    bytes.extend(compress(b""));
    bytes.extend(skippable(b"ignored"));
    bytes.extend(compress("第二行\n".as_bytes()));
    bytes.extend(skippable(b""));
    for chunk_size in [1, 7, 4096] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Zstd).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.zst".into()).await.unwrap();
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
async fn empty_frames_are_valid_but_empty_input_is_not() {
    for bytes in [compress(b""), skippable(b"metadata")] {
        let accessor = MemoryAccessor::new(&bytes, 1).layer(DecompressionLayer::new(Zstd));
        let mut reader = accessor.open("empty.zst".into()).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
    }
    let accessor = MemoryAccessor::new(&[], 1).layer(DecompressionLayer::new(Zstd));
    let mut reader = accessor.open("missing-header.zst".into()).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn corruption_truncation_and_trailing_junk_are_terminal_errors() {
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
        let polls = memory.polls.clone();
        let mut reader = memory
            .layer(DecompressionLayer::new(Zstd))
            .open("invalid.zst".into())
            .await
            .unwrap();
        assert_eq!(
            collect(reader.as_mut()).await.unwrap_err().kind(),
            ErrorKind::Decode
        );
        let before = polls.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), before);
    }
}

#[tokio::test]
async fn codec_composes_with_shared_metadata_options_ranges_and_source_errors() {
    let bytes = compress(b"payload");
    let memory = MemoryAccessor::new(&bytes, 2);
    let requests = memory.requests.clone();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    let accessor = memory.layer(DecompressionLayer::new(Zstd));
    let mut reader = accessor
        .open(ByteReadRequest::new("records.zst").with_options(options))
        .await
        .unwrap();
    assert_eq!(requests.lock().unwrap()[0].options, options);
    assert_eq!(reader.metadata().content_length, None);
    assert_eq!(reader.metadata().version.as_deref(), Some("source-version"));
    assert!(!accessor.capability().read_with_range);
    assert!(!reader.capability().read_with_range);
    assert_eq!(
        reader.read_range(0..1).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(collect(reader.as_mut()).await.unwrap(), b"payload");

    let mut memory = MemoryAccessor::new(&bytes, 2);
    memory.error = Some(ErrorKind::PermissionDenied);
    let mut reader = memory
        .layer(DecompressionLayer::new(Zstd))
        .open("failure.zst".into())
        .await
        .unwrap();
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn opening_and_partial_consumption_do_not_collect_the_whole_input() {
    let mut state = 0x1234_5678_u32;
    let bytes = (0..400_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect::<Vec<_>>();
    let compressed = compress(&bytes);
    let memory = MemoryAccessor::new(&compressed, 97);
    let polls = memory.polls.clone();
    let mut reader = memory
        .layer(DecompressionLayer::new(Zstd).with_chunk_size(NonZeroUsize::new(31).unwrap()))
        .open("large.zst".into())
        .await
        .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let first = reader.next().await.unwrap().unwrap();
    assert!(!first.is_empty() && first.len() <= 31);
    assert_eq!(first.as_ref(), &bytes[..first.len()]);
    assert!(polls.load(Ordering::SeqCst) < compressed.len().div_ceil(97));
    let before = polls.load(Ordering::SeqCst);
    drop(reader);
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), before);
}
