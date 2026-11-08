use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ByteAccessor, ByteReadRequest, ErrorKind, ListRequest};
use open_arrow_access_storage::NamedFileAccessor;
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes};

#[tokio::test]
async fn maps_names_and_lists_without_storage_io() {
    let source = MemoryAccessor::new(b"data", 2);
    let observations = source.observations.clone();
    let accessor = NamedFileAccessor::new(
        source,
        vec![
            ("b.pdf".into(), "remote/b/content".into()),
            ("a.pdf".into(), "remote/a/content".into()),
        ],
    )
    .unwrap();
    for recursive in [true, false] {
        let files: Vec<_> = accessor
            .list(ListRequest::new("").with_recursive(recursive))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            files.iter().map(|f| f.locator()).collect::<Vec<_>>(),
            vec!["b.pdf", "a.pdf"]
        );
    }
    assert!(accessor.list("subdir".into()).await.is_err());
    assert_eq!(
        accessor.open("missing".into()).await.err().unwrap().kind(),
        ErrorKind::NotFound
    );
    assert!(observations.requests.lock().unwrap().is_empty());
    let options = open_arrow_access_core::ByteReadOptions {
        io_concurrency: std::num::NonZeroUsize::new(3).unwrap(),
        ..Default::default()
    };
    let mut reader = accessor
        .open(ByteReadRequest::new("a.pdf").with_options(options))
        .await
        .unwrap();
    assert_eq!(observations.requests.lock().unwrap()[0].options, options);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "remote/a/content"
    );
    drop(accessor);
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"data");
}

#[test]
fn rejects_ambiguous_names() {
    for names in [vec![""], vec!["same", "same"]] {
        assert!(
            NamedFileAccessor::new(
                MemoryAccessor::new(b"", 2),
                names
                    .into_iter()
                    .map(|name| (name.to_owned(), "key".into()))
                    .collect()
            )
            .is_err()
        );
    }
}
