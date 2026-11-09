use open_arrow_access_service::common::MetadataLink;
mod providers;
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, ByteAccessor, Bytes, ListRequest};
use open_arrow_access_test_support::{byte_accessor, http::HttpFixture};
#[tokio::test]
async fn membership_is_lazy_ordered_and_readers_are_independent() {
    let f = HttpFixture::new();
    f.file("/content?sig=a%2Bb", Bytes::from_static(b"1234567"));
    let c = providers::connection(&f);
    let files = c
        .files(
            vec![MetadataLink {
                file_name: "paper".into(),
                url: format!("{}/content?sig=a%2Bb", f.endpoint()),
            }],
            "metadata",
        )
        .unwrap();
    assert_eq!(files.metadata(), &"metadata");
    let listed = files
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listed[0].locator(), "paper");
    assert!(f.requests().is_empty());
    assert!(files.open("missing".into()).await.is_err());
    assert!(f.requests().is_empty());
    byte_accessor::assert_read_contract(files, "paper".into(), b"1234567")
        .await
        .unwrap();
    assert_eq!(f.requests().len(), 2);
}
#[test]
fn invalid_or_duplicate_membership_fails_before_network_io() {
    let f = HttpFixture::new();
    let c = providers::connection(&f);
    assert!(
        c.files(
            vec![MetadataLink {
                file_name: "x".into(),
                url: "file:///x".into()
            }],
            ()
        )
        .is_err()
    );
    assert!(
        c.files(
            vec![
                MetadataLink {
                    file_name: "x".into(),
                    url: "https://example.com/a".into()
                },
                MetadataLink {
                    file_name: "x".into(),
                    url: "https://example.com/b".into()
                }
            ],
            ()
        )
        .is_err()
    );
    assert!(f.requests().is_empty());
}

#[tokio::test]
async fn opaque_aliases_keep_metadata_and_download_locator() {
    let f = HttpFixture::new();
    f.file("/content?sig=a%2Bb", Bytes::from_static(b"original"));
    let c = providers::connection(&f);
    let url = format!("{}/content?sig=a%2Bb", f.endpoint());
    let files = c
        .files(
            ["../opaque", "alias"]
                .into_iter()
                .map(|name| MetadataLink {
                    file_name: name.into(),
                    url: url.clone(),
                })
                .collect(),
            ("version", 4),
        )
        .unwrap();
    assert_eq!(files.metadata(), &("version", 4));
    let listed = files
        .list(ListRequest::new(""))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|object| object.locator())
            .collect::<Vec<_>>(),
        ["../opaque", "alias"]
    );
    assert!(f.requests().is_empty());
    for name in ["../opaque", "alias"] {
        let reader = files.open(name.into()).await.unwrap();
        assert!(!reader.capability().read_with_range);
        assert_eq!(
            open_arrow_access_core::read_all(reader, "test")
                .await
                .unwrap(),
            b"original"
        );
    }
    assert_eq!(f.requests().len(), 2);
    assert!(
        f.requests()
            .iter()
            .all(|request| request.path == "/content?sig=a%2Bb")
    );
}

#[tokio::test]
async fn buffered_and_streaming_downloads_enforce_the_same_body_framing() {
    use http::{Response, StatusCode};
    use open_arrow_access_core::{ByteReadOptions, ErrorKind};
    use open_arrow_access_test_support::collect_bytes;
    use opendal::{Buffer, HttpBody};

    for (declared, valid) in [
        (None, true),
        (Some("3"), true),
        (Some("2"), false),
        (Some("4"), false),
        (Some("invalid"), false),
    ] {
        let f = HttpFixture::new();
        f.handle(move |_| {
            let mut response = Response::builder().status(StatusCode::OK);
            if let Some(length) = declared {
                response = response.header("content-length", length);
            }
            // Leave SDK length checking disabled to exercise our shared framing.
            Ok(response
                .body(HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from("abc"))]),
                    None,
                ))
                .unwrap())
        });
        let connection = providers::connection(&f);
        let url = format!("{}/content", f.endpoint());
        let buffered = connection
            .bytes(url::Url::parse(&url).unwrap(), "*/*")
            .await;
        let streamed = match connection.open(&url, ByteReadOptions::default()).await {
            Ok(mut reader) => {
                let result = collect_bytes(reader.as_mut()).await.map(Bytes::from);
                assert!(reader.next().await.unwrap().is_none());
                result
            }
            Err(error) => Err(error),
        };
        for result in [buffered, streamed] {
            if valid {
                assert_eq!(result.unwrap().as_ref(), b"abc");
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Decode, "{declared:?}");
                assert_eq!(error.operation(), "provider_http");
            }
        }
    }
}
