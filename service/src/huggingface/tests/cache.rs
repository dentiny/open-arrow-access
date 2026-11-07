use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
};
use std::time::{SystemTime, UNIX_EPOCH};

use http::{Request, Response};
use open_arrow_access_core::{Accessor, ByteReadRequest, Bytes};
use opendal::{Buffer, HttpBody, HttpRedirect, HttpTransport, HttpTransporter, OperationContext};

use super::super::{HuggingFaceOptions, HuggingFaceRepository, HuggingFaceService};
use super::support::{MockHub, TOKEN};

#[derive(Clone)]
struct RedirectHub {
    files: MockHub,
    resolves: Arc<AtomicUsize>,
}

impl HttpTransport for RedirectHub {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let download = request.uri().path().contains("/resolve/");
        if download && request.extensions().get::<HttpRedirect>().is_none() {
            self.resolves.fetch_add(1, SeqCst);
        }
        let mut response = self.files.fetch(request).await?;
        if download {
            let expires = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600;
            response.extensions_mut().insert(HttpRedirect::new(
                format!("https://cdn.invalid/file?Expires={expires}")
                    .parse()
                    .unwrap(),
            ));
        }
        Ok(response)
    }
}

#[tokio::test]
async fn pinned_reads_reuse_download_addresses_but_floating_revisions_resolve_again() {
    for (revision, expected_resolves) in
        [("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1), ("main", 2)]
    {
        let hub = RedirectHub {
            files: MockHub::file("bytes.bin", Bytes::from_static(b"abcd")),
            resolves: Arc::default(),
        };
        let mut service = HuggingFaceService::new(
            HuggingFaceOptions::default()
                .with_endpoint("https://hub.invalid")
                .with_token(TOKEN)
                .with_http_downloads(),
        )
        .await
        .unwrap();
        service.context =
            OperationContext::new().with_http_transport(HttpTransporter::new(hub.clone()));
        let accessor = service
            .repository(
                HuggingFaceRepository::new("owner/example")
                    .with_root("data")
                    .with_revision(revision),
            )
            .await
            .unwrap();
        let mut first = accessor
            .open(ByteReadRequest::new("bytes.bin"))
            .await
            .unwrap();
        assert_eq!(first.read_range(0..2).await.unwrap().as_ref(), b"ab");
        let mut second = accessor
            .clone()
            .open(ByteReadRequest::new("bytes.bin"))
            .await
            .unwrap();
        assert_eq!(second.read_range(2..4).await.unwrap().as_ref(), b"cd");
        assert_eq!(hub.resolves.load(SeqCst), expected_resolves);
    }
}
