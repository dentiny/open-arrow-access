use super::*;
use crate::arxiv::{ArxivContentFormat, ArxivKind, ArxivService};
use open_arrow_access_core::{AccessorExt, ByteMetadata, Bytes, Capability, Reader, async_trait};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_test_support::{MemoryAccessor, fixture};
use std::sync::{Arc, atomic::Ordering};
use std::{num::NonZeroUsize, time::Duration};
use tokio::sync::Barrier;

struct GatedSource {
    source: MemoryAccessor,
    gate: Arc<Barrier>,
}
#[async_trait]
impl Accessor for GatedSource {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        self.source.capability()
    }
    async fn open(&self, _: Self::Request) -> Result<Box<dyn ByteReader>> {
        panic!("shard discovery must request sequential input")
    }
    async fn open_sequential(&self, request: Self::Request) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(GatedReader {
            reader: self.source.open(request).await?,
            gate: self.gate.clone(),
            position: 0,
        }))
    }
}
struct GatedReader {
    reader: Box<dyn ByteReader>,
    gate: Arc<Barrier>,
    position: usize,
}
#[async_trait]
impl Reader for GatedReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;
    fn capability(&self) -> Capability {
        self.reader.capability()
    }
    fn metadata(&self) -> &ByteMetadata {
        self.reader.metadata()
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.position == 1024 {
            // The second article header stays unavailable until the test has
            // received the first article's decoded output.
            self.gate.wait().await;
        }
        let bytes = self.reader.next().await?;
        self.position += bytes.as_ref().map_or(0, Bytes::len);
        Ok(bytes)
    }
}
#[async_trait]
impl ByteReader for GatedReader {}

#[tokio::test]
async fn a_single_shard_emits_content_before_later_headers_are_available() {
    let bytes = fixture!("arxiv-content", "latex-two.tar").await;
    let source = MemoryAccessor::new(&bytes, 512).without_content_length();
    let observed = source.observations.clone();
    let gate = Arc::new(Barrier::new(2));
    let records = ArxivService::new(GatedSource {
        source,
        gate: gate.clone(),
    })
    .records()
    .content(ArxivContentFormat::Latex)
    .layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    let byte_options = open_arrow_access_core::ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: NonZeroUsize::new(128),
    };
    let mut reader = records
        .open(
            ArxivReadRequest::new(ArxivKind::Source)
                .with_shard("src/one.tar")
                .with_byte_options(byte_options),
        )
        .await
        .unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    let first = tokio::time::timeout(Duration::from_secs(2), reader.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.schema(), crate::arxiv::content_schema());
    assert_eq!(observed.reads.load(Ordering::SeqCst), 2);
    let release = tokio::spawn(async move {
        gate.wait().await;
    });
    let mut rows = first.num_rows();
    while let Some(batch) = tokio::time::timeout(Duration::from_secs(2), reader.next())
        .await
        .unwrap()
        .unwrap()
    {
        rows += batch.num_rows();
    }
    release.await.unwrap();
    assert_eq!(rows, 2);
    assert_eq!(observed.range_reads.load(Ordering::SeqCst), 0);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].object.locator(), "src/one.tar");
    assert_eq!(requests[0].options, byte_options);
}

#[tokio::test]
async fn later_discovery_errors_are_reported_after_earlier_content() {
    let bytes = fixture!("arxiv-content", "latex-two.tar").await;
    let source = MemoryAccessor::new(&bytes, 512)
        .with_failure(1024, open_arrow_access_core::ErrorKind::PermissionDenied);
    let records = ArxivService::new(source)
        .records()
        .content(ArxivContentFormat::Latex);
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_shard("one.tar"))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        open_arrow_access_core::ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn http_shard_uses_one_get_without_header_or_article_range_requests() {
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::http::HttpFixture;
    let bytes = fixture!("arxiv-content", "latex-two.tar").await;
    let http = HttpFixture::new();
    http.respond("/src/one.tar", http::StatusCode::OK, bytes);
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(http.endpoint()))
            .unwrap()
            .with_context(http.context());
    let records = ArxivService::new(OpenDalAccessor::new(operator))
        .records()
        .content(ArxivContentFormat::Latex);
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_shard("src/one.tar"))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    let requests = http.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, http::Method::GET);
    assert_eq!(requests[0].path, "/src/one.tar");
}
