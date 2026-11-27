use std::num::NonZeroUsize;

use http::Method;
use open_arrow_access_core::{Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, read_all};
use open_arrow_access_decompression::AutoDecompressionLayer;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::http::HttpFixture;
use opendal::Operator;

async fn assert_opens(path: &str, bytes: Vec<u8>, expected: &[u8]) {
    for sequential in [false, true] {
        for (concurrency, chunk_size) in [(1, None), (1, Some(4)), (2, Some(4))] {
            let fixture = HttpFixture::new();
            fixture.file(&format!("/{path}"), bytes.clone());
            let operator =
                Operator::new(opendal_service_http::Http::default().endpoint(fixture.endpoint()))
                    .unwrap()
                    .with_context(fixture.context());
            let accessor = OpenDalAccessor::new(operator).layer(AutoDecompressionLayer::new());
            let request = ByteReadRequest::new(path).with_options(ByteReadOptions {
                io_concurrency: NonZeroUsize::new(concurrency).unwrap(),
                io_chunk_size: chunk_size.and_then(NonZeroUsize::new),
            });
            let reader = if sequential {
                accessor.open_sequential(request).await.unwrap()
            } else {
                accessor.open(request).await.unwrap()
            };
            assert_eq!(read_all(reader, "test").await.unwrap(), expected);
            let requests = fixture.requests();
            if sequential {
                assert!(requests.iter().all(|r| r.method == Method::GET));
                if chunk_size.is_none() {
                    assert_eq!(requests.len(), 1);
                    assert!(!requests[0].headers.contains_key("range"));
                } else {
                    // Explicit chunking still partitions the storage reads.
                    assert_eq!(requests[0].range(), Some(0..4));
                    assert_eq!(requests.len(), bytes.len().div_ceil(4));
                }
            } else {
                // Random-access opens keep their metadata and range semantics.
                assert_eq!(requests[0].method, Method::HEAD);
            }
        }
    }
}

#[tokio::test]
async fn plain_sequential_opens_preserve_storage_io_policy() {
    assert_opens(
        "book.rdf",
        b"original RDF metadata".to_vec(),
        b"original RDF metadata",
    )
    .await;
}

#[cfg(feature = "gzip")]
#[tokio::test]
async fn compressed_sequential_opens_preserve_storage_io_policy() {
    use open_arrow_access_test_support::fixture;
    assert_opens(
        "book.rdf.gz",
        fixture!("gzip", "payload.gz").await,
        b"payload",
    )
    .await;
}
