use std::num::NonZeroUsize;

use http::Method;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadOptions, ByteReadRequest, ListRequest, read_all,
};
use open_arrow_access_decompression::AutoDecompressionLayer;
use open_arrow_access_layers::WholeObjectLayer;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, http::HttpFixture};

fn storage(fixture: &HttpFixture) -> OpenDalAccessor {
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(fixture.endpoint()))
            .unwrap()
            .with_context(fixture.context());
    OpenDalAccessor::new(operator)
}

fn request(path: &str, concurrency: usize) -> ByteReadRequest {
    ByteReadRequest::new(path).with_options(ByteReadOptions {
        io_concurrency: NonZeroUsize::new(concurrency).unwrap(),
        io_chunk_size: NonZeroUsize::new(4),
    })
}

#[rstest::rstest]
#[tokio::test]
async fn sequential_whole_objects_use_one_unranged_get(
    #[values(false, true)] sequential: bool,
    #[values(0, 3, 17)] size: usize,
) {
    let fixture = HttpFixture::new();
    let bytes = vec![42; size];
    fixture.file("/object", bytes.clone());
    let accessor = storage(&fixture).layer(WholeObjectLayer);
    assert!(!accessor.capability().read_with_range);
    let reader = if sequential {
        accessor.open_sequential(request("object", 1)).await
    } else {
        accessor.open(request("object", 1)).await
    }
    .unwrap();
    assert!(!reader.capability().read_with_range);
    assert_eq!(read_all(reader, "test").await.unwrap(), bytes);
    let requests = fixture.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::GET);
    assert!(requests[0].range().is_none());
}

#[tokio::test]
async fn decompression_backing_opens_use_the_policy() {
    let fixture = HttpFixture::new();
    let compressed = include_bytes!("../../testdata/gzip/payload.gz");
    fixture.file("/payload.gz", compressed.as_slice());
    let accessor = storage(&fixture)
        .layer(WholeObjectLayer)
        .layer(AutoDecompressionLayer::new());
    let reader = accessor.open(request("payload.gz", 1)).await.unwrap();
    assert_eq!(read_all(reader, "test").await.unwrap(), b"payload");
    let requests = fixture.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::GET);
    assert!(requests[0].range().is_none());
}

#[tokio::test]
async fn explicit_parallel_reads_keep_partitioned_io() {
    let fixture = HttpFixture::new();
    let bytes: Vec<u8> = (0..17).collect();
    fixture.file("/object", bytes.clone());
    let accessor = storage(&fixture).layer(WholeObjectLayer);
    let reader = accessor.open(request("object", 2)).await.unwrap();
    assert_eq!(read_all(reader, "test").await.unwrap(), bytes);
    let requests = fixture.requests();
    assert_eq!(requests.len(), 5);
    assert!(requests.iter().all(|r| r.method == Method::GET));
    let mut ranges: Vec<_> = requests.iter().map(|r| r.range().unwrap()).collect();
    ranges.sort_by_key(|range| range.start);
    assert_eq!(ranges, [0..4, 4..8, 8..12, 12..16, 16..17]);
}

#[tokio::test]
async fn discovery_and_request_hints_are_preserved() {
    use futures::TryStreamExt;

    for concurrency in [1, 2] {
        let inner = MemoryAccessor::new(b"payload", 3).with_listing();
        let observations = inner.observations.clone();
        let accessor = inner.layer(WholeObjectLayer);
        assert!(accessor.capability().list);
        let listed: Vec<_> = accessor
            .list(ListRequest::new(""))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(!listed.is_empty());
        let mut requested = request("object", concurrency);
        requested.content_length = Some(7);
        let reader = accessor.open(requested.clone()).await.unwrap();
        assert_eq!(read_all(reader, "test").await.unwrap(), b"payload");
        let requests = observations.requests.lock().unwrap();
        assert_eq!(requests[0].object, requested.object);
        assert_eq!(requests[0].content_length, Some(7));
        assert_eq!(
            requests[0].options.io_concurrency,
            requested.options.io_concurrency
        );
        assert_eq!(
            requests[0].options.io_chunk_size,
            if concurrency == 1 {
                None
            } else {
                requested.options.io_chunk_size
            }
        );
    }
}
