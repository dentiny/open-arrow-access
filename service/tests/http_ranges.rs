use std::num::NonZeroUsize;

use futures::stream;
use http::{Request, Response};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReader, Bytes, ErrorKind, ReadRequest,
};
use open_arrow_access_service::common::MetadataLink;
use open_arrow_access_test_support::{
    collect_bytes, fixture,
    http::{HttpFixture, file_response},
    record::collect_column,
};
use opendal::{Buffer, HttpBody};

mod providers;

async fn open(f: &HttpFixture) -> Box<dyn ByteReader> {
    providers::connection(f)
        .open(&format!("{}/file", f.endpoint()), Default::default())
        .await
        .unwrap()
}

fn serve(request: &Request<Buffer>, bytes: Bytes) -> Response<HttpBody> {
    assert_eq!(request.headers()["accept-encoding"], "identity");
    let mut output = file_response(request, bytes);
    output
        .headers_mut()
        .insert("accept-ranges", "bytes".parse().unwrap());
    output
        .headers_mut()
        .insert("etag", "\"v1\"".parse().unwrap());
    output
}

#[tokio::test]
async fn exact_ranges_preserve_sequential_cursor() {
    let f = HttpFixture::new();
    f.handle(|request| {
        if request.headers().contains_key("range") {
            assert_eq!(request.headers()["if-match"], "\"v1\"");
        }
        Ok(serve(&request, Bytes::from_static(b"abcdefgh")))
    });
    let mut reader = providers::connection(&f)
        .open(
            &format!("{}/file", f.endpoint()),
            ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(reader.capability().read_with_range);
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"ab");
    assert_eq!(reader.read_range(5..8).await.unwrap().as_ref(), b"fgh");
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"cdefgh");
    assert_eq!(reader.read_range(0..2).await.unwrap().as_ref(), b"ab");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_or_changed_partial_responses_are_rejected() {
    for (field, value) in [
        ("status", "200"),
        ("content-range", "bytes 0-1/8"),
        ("body", "c"),
        ("etag", "\"v2\""),
        ("status", "412"),
    ] {
        let f = HttpFixture::new();
        f.handle(move |request| {
            let mut output = serve(&request, Bytes::from_static(b"abcdefgh"));
            if request.headers().contains_key("range") {
                match field {
                    "status" => *output.status_mut() = value.parse().unwrap(),
                    "body" => {
                        output.headers_mut().remove("content-length");
                        *output.body_mut() =
                            HttpBody::new(stream::iter([Ok(Buffer::from(value))]), None);
                    }
                    name => {
                        output.headers_mut().insert(name, value.parse().unwrap());
                    }
                }
            }
            Ok(output)
        });
        let mut reader = open(&f).await;
        let error = reader.read_range(2..4).await.unwrap_err();
        match (field, value) {
            ("status", "200") => assert_eq!(error.kind(), ErrorKind::Unsupported),
            ("status", code) => assert_eq!(error.http_status(), Some(code.parse().unwrap())),
            _ => assert_eq!(
                error.kind(),
                ErrorKind::Decode,
                "{field}: {value:?}: {error}"
            ),
        }
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abcdefgh");
    }
}

#[tokio::test]
async fn one_http_permit_supports_ranges_and_sequential_reads() {
    use open_arrow_access_storage::HttpConcurrencyLimitLayer;

    let f = HttpFixture::new();
    f.handle(|request| Ok(serve(&request, Bytes::from_static(b"abcdefgh"))));
    let mut reader = providers::connection(&f)
        .with_http_concurrency_limit(HttpConcurrencyLimitLayer::new(1).unwrap())
        .open(
            &format!("{}/file", f.endpoint()),
            ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        assert_eq!(reader.read_range(4..6).await.unwrap().as_ref(), b"ef");
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"ab");
        assert_eq!(reader.read_range(6..8).await.unwrap().as_ref(), b"gh");
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"cdefgh");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn remote_service_parquet_decodes_through_ranges() {
    use arrow_array::types::Int64Type;
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    let bytes = Bytes::from(fixture!("parquet", "ordered-uncompressed.parquet").await);
    let f = HttpFixture::new();
    f.handle(move |request| Ok(serve(&request, bytes.clone())));
    let files = providers::connection(&f)
        .files(
            vec![MetadataLink {
                file_name: "data.parquet".into(),
                url: format!("{}/download", f.endpoint()),
            }],
            (),
        )
        .unwrap();
    let reader = ParquetAccessor::new(files)
        .open(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    assert_eq!(
        collect_column::<Int64Type>(reader, 0).await,
        (0..7).collect::<Vec<_>>()
    );
    let requests = f.requests();
    assert!(requests.iter().any(|r| r.headers.contains_key("range")));
    assert!(requests.iter().all(|r| r.method == http::Method::GET));
}
