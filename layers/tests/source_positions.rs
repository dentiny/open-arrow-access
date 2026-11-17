#![cfg(feature = "source-positions")]
use open_arrow_access_core::{Accessor, AccessorExt, BytePosition, ErrorKind};
use open_arrow_access_layers::SourceColumnsLayer;
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture};
use std::num::NonZeroUsize;
async fn check(extension: &str, member: Vec<u8>) {
    let mut compressed = member.clone();
    compressed.extend_from_slice(&member);
    for input_chunk in [1, compressed.len()] {
        let accessor = MemoryAccessor::new(&compressed, input_chunk).layer(
            SourceColumnsLayer::new()
                .byte_positions()
                .with_chunk_size(NonZeroUsize::new(3).unwrap()),
        );
        let mut reader = accessor
            .open(format!("object.{extension}").into())
            .await
            .unwrap();

        let mut frames = [Vec::new(), Vec::new()];
        loop {
            let position = reader.position();
            let Some(bytes) = reader.next().await.unwrap() else {
                break;
            };
            let BytePosition::Frame { frame, decoded } = position.unwrap() else {
                panic!("expected frame");
            };
            let i = if frame == 0 {
                0
            } else {
                assert_eq!(frame, member.len() as u64);
                1
            };
            assert_eq!(decoded, frames[i].len() as u64);
            assert!(bytes.len() <= 3);
            frames[i].extend_from_slice(&bytes);
        }
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(frames[0], b"payload");
        assert_eq!(frames[1], frames[0]);
    }
    for data in [
        vec![],
        member[..member.len() - 1].to_vec(),
        [member.as_slice(), b"garbage", member.as_slice()].concat(),
    ] {
        let accessor =
            MemoryAccessor::new(&data, 1).layer(SourceColumnsLayer::new().byte_positions());
        let error = match accessor.open(format!("broken.{extension}").into()).await {
            Err(error) => error,
            Ok(mut reader) => {
                let error = collect_bytes(reader.as_mut()).await.unwrap_err();
                assert!(reader.next().await.unwrap().is_none());
                error
            }
        };
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(error.to_string().contains("broken"));
    }
}
#[tokio::test]
async fn gzip() {
    check("gz", fixture!("gzip", "payload.gz").await).await;
}
#[tokio::test]
async fn bz2() {
    check("bz2", fixture!("bz2", "payload.bz2").await).await;
}
#[tokio::test]
async fn zstd() {
    check("zst", fixture!("zstd", "payload.zst").await).await;
}
#[tokio::test]
async fn lz4() {
    check("lz4", fixture!("lz4", "payload.lz4").await).await;
}
