use super::*;
use crate::common::Connection;
use futures::{poll, stream};
use http::{Request, Response};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext};
use std::sync::atomic::{AtomicUsize, Ordering};

struct GateTransport {
    calls: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}
impl HttpTransport for GateTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let id = request.uri().path().rsplit('/').next().unwrap();
        if id == "blocked" {
            let permit = self.gate.acquire().await.unwrap();
            permit.forget();
        }
        let body = serde_json::to_vec(&serde_json::json!({"release_id":id,"datasets":[]})).unwrap();
        let size = body.len() as u64;
        Ok(Response::builder()
            .status(200)
            .body(HttpBody::new(
                stream::iter([Ok(Buffer::from(body))]),
                Some(size),
            ))
            .unwrap())
    }
}
fn client(
    options: ManifestCacheOptions,
) -> (
    CachedSemanticScholarService,
    Arc<AtomicUsize>,
    Arc<Semaphore>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let context =
        OperationContext::new().with_http_transport(HttpTransporter::new(GateTransport {
            calls: calls.clone(),
            gate: gate.clone(),
        }));
    let service = SemanticScholarService::with_connection(
        Connection::new("https://metadata.invalid/")
            .unwrap()
            .with_context(context),
    );
    (service.with_manifest_cache(options).unwrap(), calls, gate)
}
#[tokio::test]
async fn coalesces_equal_keys_without_blocking_other_releases() {
    let (client, calls, gate) = client(ManifestCacheOptions::default());
    let mut first = Box::pin(client.release("blocked"));
    let mut second = Box::pin(client.release("blocked"));
    assert!(poll!(first.as_mut()).is_pending());
    assert!(poll!(second.as_mut()).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client.release("other").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    gate.add_permits(1);
    let (a, b) = tokio::join!(first, second);
    assert!(Arc::ptr_eq(&a.unwrap(), &b.unwrap()));
    client.clone().release("blocked").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn cancelled_refresh_releases_its_slot_and_request_permit() {
    let (client, calls, gate) = client(ManifestCacheOptions::default());
    let mut cancelled = Box::pin(client.release("blocked"));
    assert!(poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    let mut replacement = Box::pin(client.release("blocked"));
    assert!(poll!(replacement.as_mut()).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    gate.add_permits(1);
    replacement.await.unwrap();
}
#[tokio::test]
async fn bounds_entries_and_encoded_bytes_and_does_not_retain_oversized_documents() {
    let mut options = ManifestCacheOptions::default();
    options.max_entries = NonZeroUsize::MIN;
    let (client, _, _) = client(options);
    client.release("a").await.unwrap();
    client.release("b").await.unwrap();
    assert_eq!(client.inner.state.lock().await.documents.len(), 1);
    let mut options = ManifestCacheOptions::default();
    options.max_bytes = NonZeroUsize::MIN;
    let (tiny, calls, _) = self::client(options);
    tiny.release("a").await.unwrap();
    tiny.release("a").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(tiny.inner.state.lock().await.bytes, 0);
}
fn document(value: serde_json::Value) -> ApiDocument {
    ApiDocument::from_bytes(open_arrow_access_core::Bytes::from(
        serde_json::to_vec(&value).unwrap(),
    ))
    .unwrap()
}
#[test]
fn signed_expiry_and_ttls_are_conservative() {
    let legacy = "https://files.invalid/x?Expires=1577840400&Signature=a%2Bb&AWSAccessKeyId=key";
    let sigv4 = "https://files.invalid/x?X-Amz-Date=20200101T000000Z&X-Amz-Expires=3600&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key&X-Amz-Signature=signed";
    assert_eq!(signed_expiry(legacy), Some(1577840400));
    assert_eq!(signed_expiry(sigv4), Some(1577840400));
    assert_eq!(signed_expiry(&format!("{legacy}&Expires=9")), None);
    assert_eq!(
        signed_expiry("https://files.invalid/x?Expires=1577840400"),
        None
    );
    let options = ManifestCacheOptions::default();
    let start = Instant::now();
    let now = UNIX_EPOCH + Duration::from_secs(1577840390);
    let dataset = document(serde_json::json!({"files":[legacy,sigv4]}));
    let key = Key::Dataset("2020-01-01".into(), "s2orc_v2".into());
    assert!(deadline(&key, &dataset, &options, start, now).is_none());
    assert!(
        deadline(
            &key,
            &document(serde_json::json!({"files":[legacy,"https://files.invalid/unknown"]})),
            &options,
            start,
            UNIX_EPOCH
        )
        .is_none()
    );
    assert!(
        deadline(
            &key,
            &document(serde_json::json!({"files":[]})),
            &options,
            start,
            UNIX_EPOCH
        )
        .is_none()
    );
    assert_eq!(
        deadline(
            &Key::Release("latest".into()),
            &dataset,
            &options,
            start,
            now
        ),
        start.checked_add(Duration::from_secs(30))
    );
    assert_eq!(
        deadline(
            &Key::Release("2020-01-01".into()),
            &dataset,
            &options,
            start,
            now
        ),
        start.checked_add(Duration::from_secs(300))
    );
}
#[tokio::test]
async fn errors_and_unknown_signatures_are_not_persisted_and_credentials_do_not_share_cache() {
    use open_arrow_access_test_support::http::HttpFixture;
    let fixture = HttpFixture::new();
    fixture.respond(
        "/datasets/v1/release/error",
        http::StatusCode::UNAUTHORIZED,
        b"{}".to_vec(),
    );
    fixture.respond(
        "/datasets/v1/release/2026-10-27/dataset/s2orc_v2",
        http::StatusCode::OK,
        br#"{"name":"s2orc_v2","files":["https://files.invalid/unknown"]}"#.to_vec(),
    );
    fixture.respond(
        "/datasets/v1/release/2026-10-27",
        http::StatusCode::OK,
        br#"{"release_id":"2026-10-27","datasets":[]}"#.to_vec(),
    );
    let connection = Connection::new(fixture.endpoint())
        .unwrap()
        .with_context(fixture.context());
    let first = SemanticScholarService::with_connection(connection.clone())
        .with_key("first-key")
        .unwrap()
        .with_manifest_cache(ManifestCacheOptions::default())
        .unwrap();
    let second = SemanticScholarService::with_connection(connection)
        .with_key("second-key")
        .unwrap()
        .with_manifest_cache(ManifestCacheOptions::default())
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            first.release("error").await.unwrap_err().http_status(),
            Some(401)
        );
        first
            .dataset_manifest("2026-10-27", "s2orc_v2")
            .await
            .unwrap();
    }
    first.release("2026-10-27").await.unwrap();
    second.release("2026-10-27").await.unwrap();
    assert_eq!(fixture.requests().len(), 6);
    assert!(!format!("{first:?}").contains("first-key"));
}
