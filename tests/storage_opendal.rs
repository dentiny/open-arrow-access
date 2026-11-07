use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, ByteAccessor, Bytes, ErrorKind, ListRequest,
    storage::{OpenDalAccessor, opendal},
};

#[tokio::test]
async fn generic_listing_accepts_implicit_prefixes_and_honors_recursion() {
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    for path in ["metadata/a.json", "metadata/nested/b.json", "other/c.json"] {
        operator
            .write(path, Bytes::from_static(b"{}"))
            .await
            .unwrap();
    }
    let bytes = OpenDalAccessor::new(operator);
    assert!(bytes.capability().list);
    for prefix in ["metadata", "metadata/"] {
        let mut files = bytes
            .list(prefix.into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        files.sort_by(|a, b| a.locator().cmp(b.locator()));
        assert_eq!(
            files,
            vec!["metadata/a.json".into(), "metadata/nested/b.json".into()]
        );
        let mut request = ListRequest::new(prefix);
        request.recursive = false;
        let files = bytes
            .list(request)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(files, vec!["metadata/a.json".into()]);
    }
    let all = bytes
        .list("".into())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn generic_access_rejects_locator_aliases_before_backend_normalization() {
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    operator
        .write("metadata/a.json", Bytes::from_static(b"{}"))
        .await
        .unwrap();
    let bytes = OpenDalAccessor::new(operator);
    for path in [
        " metadata/a.json",
        "metadata/a.json ",
        "/metadata/a.json",
        "metadata//a.json",
        "metadata/./a.json",
        "metadata/../metadata/a.json",
        "metadata/a.json/",
        "",
    ] {
        assert_eq!(
            bytes.open(path.into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput,
            "{path}"
        );
    }
    for prefix in [
        " metadata",
        "metadata ",
        "/metadata",
        "metadata//",
        "./metadata",
        "metadata/../other",
    ] {
        assert_eq!(
            bytes.list(prefix.into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput,
            "{prefix}"
        );
    }
    let mut reader = bytes.open("metadata/a.json".into()).await.unwrap();
    assert_eq!(
        reader.next().await.unwrap().unwrap(),
        Bytes::from_static(b"{}")
    );
    assert!(reader.next().await.unwrap().is_none());
}
