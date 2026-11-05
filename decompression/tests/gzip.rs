#![cfg(feature = "gzip")]

mod common;

use common::{MemoryAccessor, collect};
use flate2::{Compression, write::GzEncoder};
use futures::StreamExt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, Error, ErrorKind,
};
use open_arrow_access_decompression::{DecompressionLayer, Gzip};
use std::{io::Write, num::NonZeroUsize, sync::atomic::Ordering};

fn compress(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn concatenated_members_decode_in_order_across_single_byte_input_chunks() {
    let mut input = compress(b"first\n");
    input.extend(compress(b""));
    input.extend(compress("第二行\n".as_bytes()));
    let accessor = MemoryAccessor::new(&input, 1)
        .layer(DecompressionLayer::new(Gzip).with_chunk_size(NonZeroUsize::new(3).unwrap()));
    let mut reader = accessor.open("records.gz".into()).await.unwrap();
    let mut output = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        assert!(!chunk.is_empty() && chunk.len() <= 3);
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, "first\n第二行\n".as_bytes());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn metadata_capabilities_options_and_discovery_follow_the_wrapped_interface() {
    let memory = MemoryAccessor::new(&compress(b"payload"), 2);
    let requests = memory.requests.clone();
    let accessor = memory.layer(DecompressionLayer::new(Gzip));
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(4).unwrap(),
        io_chunk_size: NonZeroUsize::new(17),
    };
    let mut reader = accessor
        .open(ByteReadRequest::new("data.gz").with_options(options))
        .await
        .unwrap();
    assert!(accessor.capability().read);
    assert!(accessor.capability().list);
    assert!(!accessor.capability().read_with_range);
    assert!(!reader.capability().read_with_range);
    assert!(!reader.capability().list);
    assert_eq!(reader.metadata().content_length, None);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "data.gz"
    );
    assert_eq!(reader.metadata().version.as_deref(), Some("source-version"));
    assert_eq!(requests.lock().unwrap()[0].options, options);
    assert_eq!(
        reader.read_range(0..1).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        reader
            .read_ranges(vec![0..1, 1..2])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(collect(reader.as_mut()).await.unwrap(), b"payload");
    let mut objects = accessor.list("prefix".into()).await.unwrap();
    assert_eq!(objects.next().await.unwrap().unwrap().locator(), "prefix");
    assert!(objects.next().await.is_none());
}

#[tokio::test]
async fn valid_empty_member_is_eof_but_empty_compressed_input_is_invalid() {
    let accessor = MemoryAccessor::new(&compress(b""), 1).layer(DecompressionLayer::new(Gzip));
    let mut reader = accessor.open("empty.gz".into()).await.unwrap();
    assert!(reader.next().await.unwrap().is_none());
    let accessor = MemoryAccessor::new(&[], 1).layer(DecompressionLayer::new(Gzip));
    let mut reader = accessor.open("missing-header.gz".into()).await.unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_truncated_checksum_and_trailing_data_fail_terminally() {
    let valid = compress(b"payload");
    let mut corrupt = valid.clone();
    let crc = corrupt.len() - 8;
    corrupt[crc] ^= 1;
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b"trailing junk");
    let mut truncated_member = valid.clone();
    truncated_member.extend_from_slice(&valid[..valid.len() - 1]);
    let cases = [
        b"not gzip".to_vec(),
        valid[..3].to_vec(),
        valid[..valid.len() - 1].to_vec(),
        corrupt,
        trailing,
        truncated_member,
    ];
    for bytes in cases {
        let memory = MemoryAccessor::new(&bytes, 2);
        let polls = memory.polls.clone();
        let accessor = memory.layer(DecompressionLayer::new(Gzip));
        let mut reader = accessor.open("invalid.gz".into()).await.unwrap();
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
async fn source_errors_keep_their_category_and_original_source() {
    for kind in [
        ErrorKind::Io,
        ErrorKind::PermissionDenied,
        ErrorKind::NotFound,
    ] {
        let mut memory = MemoryAccessor::new(&compress(b"payload"), 2);
        memory.error = Some(kind);
        let accessor = memory.layer(DecompressionLayer::new(Gzip));
        let mut reader = accessor.open("source.gz".into()).await.unwrap();
        let error = collect(reader.as_mut()).await.unwrap_err();
        assert_eq!(error.kind(), kind);
        let io = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        let original = io.get_ref().unwrap().downcast_ref::<Error>().unwrap();
        assert_eq!(original.operation(), "upstream");
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn opening_is_lazy_output_is_bounded_and_drop_stops_source_reads() {
    let body = vec![b'a'; 1_000_000];
    let compressed = compress(&body);
    let memory = MemoryAccessor::new(&compressed, 1);
    let polls = memory.polls.clone();
    let accessor =
        memory.layer(DecompressionLayer::new(Gzip).with_chunk_size(NonZeroUsize::new(31).unwrap()));
    let mut reader = accessor.open("large.gz".into()).await.unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let first = reader.next().await.unwrap().unwrap();
    assert!(!first.is_empty() && first.len() <= 31);
    assert!(polls.load(Ordering::SeqCst) < compressed.len());
    drop(reader);
    let before = polls.load(Ordering::SeqCst);
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn separate_opens_and_nested_layers_have_independent_decoder_state() {
    let memory = MemoryAccessor::new(&compress(&compress(b"nested")), 2);
    let accessor = memory
        .layer(DecompressionLayer::new(Gzip))
        .layer(DecompressionLayer::new(Gzip));
    let mut first = accessor.open("nested.gz.gz".into()).await.unwrap();
    let mut second = accessor.open("nested.gz.gz".into()).await.unwrap();
    assert_eq!(collect(first.as_mut()).await.unwrap(), b"nested");
    assert_eq!(collect(second.as_mut()).await.unwrap(), b"nested");
}
