use super::*;
use crate::arxiv::{ArxivContentFormat, ArxivKind, ArxivService};
use crate::common::Connection;
use futures::StreamExt;
use open_arrow_access_core::{AccessorExt, ByteReadOptions, ErrorKind};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_test_support::{MemoryAccessor, fixture, http::HttpFixture};
use std::{num::NonZeroUsize, sync::atomic::Ordering};

async fn manifest_service(
    http: &HttpFixture,
    count: usize,
    size: u64,
) -> ArxivService<open_arrow_access_storage::OpenDalAccessor> {
    let bytes = fixture!("arxiv-content", "latex-two.tar").await;
    let mut manifest = String::from("<arXivSRC>");
    // The other month must be filtered without opening its TAR.
    for (month, sequence) in (1..=count)
        .map(|sequence| (2410, sequence))
        .chain([(2411, 1)])
    {
        let key = format!("src/arXiv_src_{month}_{sequence:03}.tar");
        manifest.push_str(&format!("<file><filename>{key}</filename><yymm>{month}</yymm><seq_num>{sequence}</seq_num><size>{size}</size><num_items>2</num_items><md5sum>00000000000000000000000000000000</md5sum></file>"));
        if month == 2410 {
            http.file(&format!("/{key}"), bytes.clone());
        }
    }
    manifest.push_str("</arXivSRC>");
    http.file("/src/arXiv_src_manifest.xml", manifest.into_bytes());
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(http.endpoint()))
            .unwrap()
            .with_context(http.context());
    ArxivService::new(open_arrow_access_storage::OpenDalAccessor::new(operator))
        .with_metadata_connection(metadata_connection(http).await)
}

#[tokio::test]
async fn manifest_content_splits_single_tar_but_keeps_multiple_tars_independent() {
    use open_arrow_access_core::ScanPlanner;
    for count in [0, 1, 2] {
        let http = HttpFixture::new();
        let records = manifest_service(&http, count, 1)
            .await
            .records()
            .content(ArxivContentFormat::Latex);
        let mut plan = records
            .plan_scan(ArxivReadRequest::new(ArxivKind::Source).with_month(2410))
            .await
            .unwrap();
        assert_eq!(plan.streaming_discovery, count == 1);
        assert!(
            http.requests()
                .iter()
                .all(|request| request.path == "/src/arXiv_src_manifest.xml")
        );
        let mut partition_rows = Vec::new();
        while let Some(partition) = plan.partitions.next().await {
            let mut reader = partition.unwrap().await.unwrap();
            let mut rows = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                assert_eq!(batch["metadata"].null_count(), 0);
                rows += batch.num_rows();
            }
            partition_rows.push(rows);
        }
        assert_eq!(
            partition_rows,
            match count {
                0 => vec![],
                1 => vec![1, 1],
                _ => vec![2, 2],
            }
        );
        for sequence in 1..=count {
            let key = format!("/src/arXiv_src_2410_{sequence:03}.tar");
            assert_eq!(
                http.requests()
                    .iter()
                    .filter(|request| request.method == http::Method::GET && request.path == key)
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn manifest_article_discovery_retains_length_validation() {
    let http = HttpFixture::new();
    let size = fixture!("arxiv-content", "latex-two.tar").await.len() as u64 + 1;
    let records = manifest_service(&http, 1, size)
        .await
        .records()
        .content(ArxivContentFormat::Latex)
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ));
    let mut reader = records
        .open(ArxivReadRequest::new(ArxivKind::Source).with_month(2410))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("smaller than manifest payload size"),
        "{error}"
    );
    assert!(reader.next().await.unwrap().is_none());
    assert!(
        http.requests()
            .iter()
            .all(|request| !request.path.starts_with("/oai"))
    );
}

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
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch["metadata"].null_count(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
    let requests = http.requests();
    assert_eq!(requests.len(), 3); // One shard GET, two serialized OAI lookups.
    assert!(requests.iter().all(|r| r.method == http::Method::GET));
    assert_eq!(
        requests.iter().filter(|r| r.path == "/src/one.tar").count(),
        1
    );
}
