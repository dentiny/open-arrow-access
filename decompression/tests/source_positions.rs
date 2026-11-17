#![cfg(any(feature = "gzip", feature = "bz2", feature = "zstd", feature = "lz4"))]
use open_arrow_access_core::{
    Accessor, AccessorExt, BytePosition, ByteReadOptions, ByteReadRequest, ErrorKind,
};
use open_arrow_access_decompression::{Codec, DecompressionLayer};
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture};
use std::num::NonZeroUsize;

async fn check(codec: impl Codec, member: Vec<u8>) {
    let mut compressed = member.clone();
    compressed.extend_from_slice(&member);
    for input_chunk in [1, compressed.len()] {
        let accessor = MemoryAccessor::new(&compressed, input_chunk).layer(
            DecompressionLayer::new(codec.clone()).with_chunk_size(NonZeroUsize::new(3).unwrap()),
        );
        let request = ByteReadRequest::new("object").with_options(ByteReadOptions {
            track_positions: true,
            ..Default::default()
        });
        let mut reader = accessor.open(request).await.unwrap();
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
        let mut reader = accessor.open("object".into()).await.unwrap();
        assert_eq!(reader.position(), None);
        assert_eq!(
            collect_bytes(reader.as_mut()).await.unwrap(),
            b"payloadpayload"
        );
    }
    for data in [
        vec![],
        member[..member.len() - 1].to_vec(),
        [member.as_slice(), b"garbage", member.as_slice()].concat(),
    ] {
        let accessor = MemoryAccessor::new(&data, 1).layer(DecompressionLayer::new(codec.clone()));
        let request = ByteReadRequest::new("broken").with_options(ByteReadOptions {
            track_positions: true,
            ..Default::default()
        });
        let error = match accessor.open(request).await {
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
#[cfg(feature = "gzip")]
#[tokio::test]
async fn gzip() {
    check(
        open_arrow_access_decompression::gzip::Gzip,
        fixture!("gzip", "payload.gz").await,
    )
    .await;
}
#[cfg(feature = "bz2")]
#[tokio::test]
async fn bz2() {
    check(
        open_arrow_access_decompression::bz2::Bzip2,
        fixture!("bz2", "payload.bz2").await,
    )
    .await;
}
#[cfg(feature = "zstd")]
#[tokio::test]
async fn zstd() {
    check(
        open_arrow_access_decompression::zstd::Zstd,
        fixture!("zstd", "payload.zst").await,
    )
    .await;
}
#[cfg(feature = "lz4")]
#[tokio::test]
async fn lz4() {
    check(
        open_arrow_access_decompression::lz4::Lz4,
        fixture!("lz4", "payload.lz4").await,
    )
    .await;
}
