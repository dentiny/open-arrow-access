#![cfg(feature = "bz2")]

mod common;

use std::{io::Write, num::NonZeroUsize, sync::atomic::Ordering};

use bzip2::{Compression, write::BzEncoder};
use open_arrow_access_core::{Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, ErrorKind};
use open_arrow_access_decompression::{Bzip2, DecompressionLayer};

use common::{MemoryAccessor, collect};

fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn concatenated_members_preserve_order_with_fragmented_input_and_bounded_output() {
    let mut bytes = compress(b"first\n");
    bytes.extend(compress(b""));
    bytes.extend(compress("第二行\n".as_bytes()));
    for chunk_size in [1, 7, bytes.len()] {
        let accessor = MemoryAccessor::new(&bytes, chunk_size)
            .layer(DecompressionLayer::new(Bzip2).with_chunk_size(NonZeroUsize::new(3).unwrap()));
        let mut reader = accessor.open("records.bz2".into()).await.unwrap();
        let mut output = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            assert!(!chunk.is_empty() && chunk.len() <= 3);
            output.extend_from_slice(&chunk);
        }
        assert_eq!(output, "first\n第二行\n".as_bytes());
        assert!(reader.next().await.unwrap().is_none());
    }
    let accessor = MemoryAccessor::new(&compress(b""), 1).layer(DecompressionLayer::new(Bzip2));
    let mut reader = accessor.open("empty.bz2".into()).await.unwrap();
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn invalid_truncated_checksum_and_trailing_input_fail_terminally() {
    let valid = compress(b"payload");
    let mut block_crc = valid.clone();
    block_crc[10] ^= 1;
    let mut stream_crc = valid.clone();
    let crc_byte = stream_crc.len() - 2;
    stream_crc[crc_byte] ^= 0x80;
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"trailing junk");
    let mut truncated_member = valid.clone();
    truncated_member.extend_from_slice(&valid[..valid.len() - 1]);
    for bytes in [
        vec![],
        b"not bzip2".to_vec(),
        valid[..3].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        block_crc,
        stream_crc,
        trailing,
        truncated_member,
    ] {
        let input = MemoryAccessor::new(&bytes, 1);
        let polls = input.polls.clone();
        let mut reader = input
            .layer(DecompressionLayer::new(Bzip2))
            .open("bad.bz2".into())
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
async fn codec_preserves_options_metadata_and_source_errors() {
    let bytes = compress(b"payload");
    let input = MemoryAccessor::new(&bytes, 2);
    let requests = input.requests.clone();
    let accessor = input.layer(DecompressionLayer::new(Bzip2));
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    let mut reader = accessor
        .open(ByteReadRequest::new("input.bz2").with_options(options))
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

    let mut input = MemoryAccessor::new(&bytes, 2);
    input.error = Some(ErrorKind::PermissionDenied);
    let mut reader = input
        .layer(DecompressionLayer::new(Bzip2))
        .open("failure.bz2".into())
        .await
        .unwrap();
    assert_eq!(
        collect(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn opening_and_partial_reading_are_lazy_and_drop_stops_work() {
    let mut state = 0x1234_5678u32;
    let body = (0..400_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect::<Vec<_>>();
    let bytes = compress(&body);
    let input = MemoryAccessor::new(&bytes, 97);
    let polls = input.polls.clone();
    let mut reader = input
        .layer(DecompressionLayer::new(Bzip2).with_chunk_size(NonZeroUsize::new(31).unwrap()))
        .open("large.bz2".into())
        .await
        .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let first = reader.next().await.unwrap().unwrap();
    assert!(!first.is_empty() && first.len() <= 31);
    assert_eq!(first.as_ref(), &body[..first.len()]);
    assert!(polls.load(Ordering::SeqCst) < bytes.len().div_ceil(97));
    let before = polls.load(Ordering::SeqCst);
    drop(reader);
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), before);
}
