#![cfg(feature = "source-positions")]
use open_arrow_access_core::{Accessor, AccessorExt, BytePosition, ErrorKind};
use open_arrow_access_layers::SourcePositionLayer;
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture};
use std::num::NonZeroUsize;
async fn check(extension: &str, member: Vec<u8>) {
    let mut compressed = member.clone();
    compressed.extend_from_slice(&member);
    for input_chunk in [1, compressed.len()] {
        let accessor = MemoryAccessor::new(&compressed, input_chunk)
            .layer(SourcePositionLayer::default().with_chunk_size(NonZeroUsize::new(3).unwrap()));
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
        let accessor = MemoryAccessor::new(&data, 1).layer(SourcePositionLayer::default());
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

#[tokio::test]
async fn sequential_input_overrides_network_splitting_but_retains_frame_positions() {
    use open_arrow_access_core::{
        ByteAccessor, ByteReadRequest, ByteReader, Capability, Result, async_trait,
    };
    struct Sequential(MemoryAccessor);
    #[async_trait]
    impl Accessor for Sequential {
        type Request = ByteReadRequest;
        type Reader = dyn ByteReader;
        fn capability(&self) -> Capability {
            self.0.capability()
        }
        async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
            panic!("whole-file input must use the sequential API")
        }
        async fn open_sequential(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
            assert!(request.options.io_chunk_size.is_none());
            assert_eq!(request.options.io_concurrency.get(), 1);
            self.0.open(request).await
        }
    }
    #[async_trait]
    impl ByteAccessor for Sequential {}
    let member = fixture!("gzip", "payload.gz").await;
    let input = [member.as_slice(), member.as_slice()].concat();
    let source = Sequential(MemoryAccessor::new(&input, 3)).layer(SourcePositionLayer::default());
    let mut request = ByteReadRequest::new("object.gz");
    request.options.io_chunk_size = NonZeroUsize::new(64 * 1024 * 1024);
    request.options.io_concurrency = NonZeroUsize::new(8).unwrap();
    let mut reader = source.open(request).await.unwrap();
    let mut bytes = Vec::new();
    let mut frames = Vec::new();
    while let Some(position) = reader.position() {
        let Some(chunk) = reader.next().await.unwrap() else {
            break;
        };
        let BytePosition::Frame { frame, .. } = position else {
            panic!("frame position required")
        };
        frames.push(frame);
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes, b"payloadpayload");
    assert!(frames.contains(&0));
    assert!(frames.contains(&(member.len() as u64)));
}
