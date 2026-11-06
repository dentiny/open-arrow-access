pub fn fixture(format: &str, name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("testdata")
        .join(format)
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

use open_arrow_access_core::{Accessor, AccessorExt, ByteReader, Error, ErrorKind, Result};
use open_arrow_access_decompression::{Codec, DecompressionLayer};
use open_arrow_access_test_support::MemoryAccessor;
use std::sync::atomic::{AtomicUsize, Ordering};

pub async fn collect(reader: &mut dyn ByteReader) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        assert!(!bytes.is_empty());
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

fn upstream_error(error: &Error) -> &Error {
    use std::error::Error as _;
    let mut source = error.source().expect("missing decoder source");
    loop {
        if let Some(shared) = source.downcast_ref::<Error>()
            && shared.operation() == "memory_read"
        {
            return shared;
        }
        source = match source
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            Some(inner) => inner,
            None => source.source().expect("missing original upstream error"),
        };
    }
}

pub async fn assert_terminal_error(
    reader: &mut dyn ByteReader,
    reads: &AtomicUsize,
    kind: ErrorKind,
) -> Error {
    use std::error::Error as _;
    let error = collect(reader).await.unwrap_err();
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), "decompress");
    assert!(error.source().is_some());
    let calls = reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reads.load(Ordering::SeqCst), calls);
    error
}

pub async fn error_contract<C: Codec>(codec: C, format: &str, payload: &str) -> Result<()> {
    use std::error::Error as _;
    let bytes = fixture(format, payload);
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        // Fail before a header, within it, and while checking EOF after a valid stream.
        for offset in [0, 1, bytes.len()] {
            let input = MemoryAccessor::new(&bytes, 1).with_failure(offset, kind);
            let observed = input.observations.clone();
            let mut reader = input
                .layer(DecompressionLayer::new(codec.clone()))
                .open("source".into())
                .await?;
            assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
            let error = assert_terminal_error(reader.as_mut(), &observed.reads, kind).await;
            let original = upstream_error(&error);
            assert_eq!(original.kind(), kind);
            assert_eq!(original.to_string(), "memory_read: injected source failure");
            let cause = original
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap();
            assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
            assert_eq!(cause.to_string(), "injected source failure");
        }
    }
    for offset in [0, 1, bytes.len()] {
        let input = MemoryAccessor::new(&bytes, 1).with_empty_chunk(offset);
        let observed = input.observations.clone();
        let mut reader = input
            .layer(DecompressionLayer::new(codec.clone()))
            .open("empty-chunk".into())
            .await?;
        let error = assert_terminal_error(reader.as_mut(), &observed.reads, ErrorKind::Io).await;
        assert!(error.source().unwrap().to_string().contains("empty chunk"));
    }
    let input = MemoryAccessor::new(&bytes, 1);
    let mut reader = input
        .layer(DecompressionLayer::new(codec))
        .open("sequential".into())
        .await?;
    let error = reader.read_range(0..1).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.operation(), "read_range");
    let error = reader.read_ranges(vec![0..1, 1..2]).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(reader.read_ranges(vec![]).await?.is_empty());
    assert_eq!(collect(reader.as_mut()).await?, b"payload");
    Ok(())
}
