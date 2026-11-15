use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::stream;
use http::{Request, Response};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReader, Bytes, ErrorKind, ReadRequest,
};
use open_arrow_access_service::common::MetadataLink;
use open_arrow_access_test_support::{
    DropProbe, collect_bytes, fixture,
    http::{HttpFixture, file_response},
    record::collect_column,
};
use opendal::{Buffer, HttpBody};
use tokio::sync::{Notify, Semaphore};

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
    let requests = f.requests().len();
    assert!(reader.read_ranges(vec![]).await.unwrap().is_empty());
    assert_eq!(
        reader.read_ranges(vec![0..0, 8..8]).await.unwrap(),
        vec![Bytes::new(); 2]
    );
    assert_eq!(f.requests().len(), requests);
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
        ("content-encoding", "gzip"),
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
        let error = reader.read_ranges(vec![2..4, 4..4]).await.unwrap_err();
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
async fn multiple_ranges_use_bounded_concurrency_and_return_in_request_order() {
    let f = HttpFixture::new();
    let active = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let second_done = Arc::new(Notify::new());
    let completed = Arc::new(Mutex::new(Vec::new()));
    f.handle({
        let (active, started, release, second_done, completed) = (
            active.clone(),
            started.clone(),
            release.clone(),
            second_done.clone(),
            completed.clone(),
        );
        move |request| {
            let bytes = Bytes::from_static(b"abcdefgh");
            let mut output = serve(&request, bytes.clone());
            if let Some(range) = request.headers().get("range") {
                assert_eq!(request.headers()["if-match"], "\"v1\"");
                let (start, _) = range
                    .to_str()
                    .unwrap()
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                let start = start.parse::<usize>().unwrap();
                let (active, started, release, second_done, completed) = (
                    active.clone(),
                    started.clone(),
                    release.clone(),
                    second_done.clone(),
                    completed.clone(),
                );
                *output.body_mut() = HttpBody::new(
                    stream::once(Box::pin(async move {
                        if active.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                            started.notify_one();
                        }
                        release.acquire().await.unwrap().forget();
                        if start == 0 {
                            second_done.notified().await;
                        } else if start == 2 {
                            second_done.notify_one();
                        }
                        completed.lock().unwrap().push(start);
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(Buffer::from(bytes.slice(start..start + 2)))
                    })),
                    Some(2),
                );
            }
            Ok(output)
        }
    });
    let mut reader = providers::connection(&f)
        .open(
            &format!("{}/file", f.endpoint()),
            ByteReadOptions {
                io_concurrency: NonZeroUsize::new(2).unwrap(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let read = reader.read_ranges(vec![0..2, 2..4, 4..6, 6..8]);
    tokio::pin!(read);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut read => panic!("read finished before body release: {result:?}"),
            _ = started.notified() => {}
        }
        assert_eq!(active.load(Ordering::SeqCst), 2);
        assert_eq!(f.requests().len(), 3); // initial GET and two range requests
        release.add_permits(4);
        assert_eq!(
            read.await.unwrap(),
            [b"ab", b"cd", b"ef", b"gh"].map(|bytes| Bytes::from_static(bytes))
        );
    })
    .await
    .unwrap();
    assert_eq!(completed.lock().unwrap()[0], 2); // second request completed first
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn range_error_drops_in_flight_bodies_without_opening_later_ranges() {
    let f = HttpFixture::new();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    f.handle({
        let (started, dropped) = (started.clone(), dropped.clone());
        move |request| {
            let mut output = serve(&request, Bytes::from_static(b"abcdefgh"));
            match request
                .headers()
                .get("range")
                .and_then(|value| value.to_str().ok())
            {
                Some("bytes=2-3") => {
                    let started = started.clone();
                    *output.body_mut() = HttpBody::new(
                        stream::once(Box::pin(async move {
                            started.notified().await;
                            Ok(Buffer::from("c")) // shorter than the validated range
                        })),
                        None,
                    );
                }
                Some("bytes=0-1") => {
                    let started = started.clone();
                    let probe = DropProbe::new(dropped.clone());
                    *output.body_mut() = HttpBody::new(
                        stream::once(Box::pin(async move {
                            let _probe = probe;
                            started.notify_one();
                            futures::future::pending().await
                        })),
                        Some(2),
                    );
                }
                Some(range) => panic!("later range unexpectedly opened: {range}"),
                None => {}
            }
            Ok(output)
        }
    });
    let mut reader = providers::connection(&f)
        .open(
            &format!("{}/file", f.endpoint()),
            ByteReadOptions {
                io_concurrency: NonZeroUsize::new(2).unwrap(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        reader.read_ranges(vec![0..2, 2..4, 4..6]),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(f.requests().len(), 3);
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
