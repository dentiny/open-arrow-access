use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest};
use open_arrow_access_service::ResolvedFileSet;
use open_arrow_access_storage::NamedFileAccessor;
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes};

#[tokio::test]
async fn retains_custom_metadata_and_delegates_to_the_file_namespace() {
    let source = MemoryAccessor::new(b"data", 2);
    let observed = source.observations.clone();
    let files =
        NamedFileAccessor::new(source, vec![("paper.pdf".into(), "objects/42".into())]).unwrap();
    let resolved = ResolvedFileSet::new(files, "published-version");
    assert_eq!(resolved.metadata(), &"published-version");
    assert!(resolved.capability().list);
    let listed: Vec<_> = resolved
        .list("".into())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed[0].locator(), "paper.pdf");
    assert!(observed.requests.lock().unwrap().is_empty());

    let options = ByteReadOptions {
        io_chunk_size: std::num::NonZeroUsize::new(2),
        ..Default::default()
    };
    let mut reader = resolved
        .open(ByteReadRequest::new("paper.pdf").with_options(options))
        .await
        .unwrap();
    {
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests[0].object.locator(), "objects/42");
        assert_eq!(requests[0].options, options);
    }
    drop(resolved);
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"data");
}
