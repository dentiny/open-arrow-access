use super::*;
use crate::arxiv::{ArxivContentFormat, ArxivKind, ArxivService};
use crate::common::Connection;
use open_arrow_access_core::{AccessorExt, ByteReadOptions, ErrorKind};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_test_support::{MemoryAccessor, fixture, http::HttpFixture};
use std::{num::NonZeroUsize, sync::atomic::Ordering};

async fn metadata_connection(http: &HttpFixture) -> Connection {
    for (id, file) in [
        ("2410.00001", "metadata-record.xml"),
        ("2410.00002", "metadata-second.xml"),
    ] {
        let path = format!(
            "/oai?verb=GetRecord&metadataPrefix=arXivRaw&identifier=oai%3AarXiv.org%3A{id}"
        );
        http.respond(
            &path,
            http::StatusCode::OK,
            fixture!("arxiv-content", file).await,
        );
    }
    Connection::new(http.endpoint())
        .unwrap()
        .with_context(http.context())
}

#[tokio::test]
async fn first_article_precedes_later_discovery_errors() {
    let bytes = fixture!("arxiv-content", "latex-two.tar").await;
    let source = MemoryAccessor::new(&bytes, 512)
        .without_content_length()
        .with_failure(1024, ErrorKind::PermissionDenied);
    let observed = source.observations.clone();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: NonZeroUsize::new(128),
    };
    let http = HttpFixture::new();
    let records = ArxivService::new(source)
        .with_metadata_connection(metadata_connection(&http).await)
        .records()
        .content(ArxivContentFormat::Latex);
    let mut reader = records
        .open(
            ArxivReadRequest::new(ArxivKind::Source)
                .with_shard("one.tar")
                .with_byte_options(options),
        )
        .await
        .unwrap();
    assert!(observed.requests.lock().unwrap().is_empty());
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(observed.reads.load(Ordering::SeqCst), 2);
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(observed.range_reads.load(Ordering::SeqCst), 0);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].options, options);
}

#[tokio::test]
async fn http_shard_uses_one_get_for_parallel_content() {
    use open_arrow_access_storage::OpenDalAccessor;
    let http = HttpFixture::new();
    http.respond(
        "/src/one.tar",
        http::StatusCode::OK,
        fixture!("arxiv-content", "latex-two.tar").await,
    );
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(http.endpoint()))
            .unwrap()
            .with_context(http.context());
    let records = ArxivService::new(OpenDalAccessor::new(operator))
        .with_metadata_connection(metadata_connection(&http).await)
        .records()
        .content(ArxivContentFormat::Latex)
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ));
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_shard("src/one.tar"))
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch["metadata"].null_count(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    let requests = http.requests();
    more_asserts::assert_ge!(started.elapsed(), std::time::Duration::from_secs(3));
    assert_eq!(requests.len(), 3); // One shard GET, two serialized OAI lookups.
    assert!(requests.iter().all(|r| r.method == http::Method::GET));
    assert_eq!(
        requests.iter().filter(|r| r.path == "/src/one.tar").count(),
        1
    );
}
