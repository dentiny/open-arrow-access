#[path = "commoncrawl/support.rs"]
mod commoncrawl_support;

use commoncrawl_support::{Records, entry, manifest};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadOptionsProvider, into_stream,
};
use open_arrow_access_service::warc::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest};
use std::num::NonZeroUsize;

const SEGMENT: &str = "1664030338280.51";

fn request() -> CommonCrawlWarcRequest {
    CommonCrawlWarcRequest::new("CC-MAIN-2022-40").unwrap()
}

#[test]
fn dataset_request_uses_the_crawl_inventory_and_mirror_origin() {
    let request =
        CommonCrawlWarcRequest::from_url("https://mirror.example/crawl-data/CC-MAIN-2022-40/")
            .unwrap();
    assert_eq!(request.crawl(), "CC-MAIN-2022-40");
    assert_eq!(request.origin(), "https://mirror.example");
    assert_eq!(
        request.manifest().locator(),
        "crawl-data/CC-MAIN-2022-40/warc.paths.gz"
    );
}

#[tokio::test]
async fn dataset_reads_all_segments_in_manifest_order_and_forwards_options() {
    let manifests = manifest("crawl.paths", 1);
    let manifest_opens = manifests.observations.clone();
    let records = Records::default();
    let opens = records.opens.clone();
    let accessor = CommonCrawlWarcAccessor::new(manifests, records);
    let mut selection = request().with_byte_options(ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(4),
    });
    selection.read_options_mut().batch_size = NonZeroUsize::new(2).unwrap();
    let reader = accessor.open(selection).await.unwrap();
    assert!(reader.metadata().source.is_none());
    assert_eq!(reader.metadata().schema.metadata()["fixture"], "provider");
    assert_eq!(opens.lock().unwrap().len(), 1);
    let paths = commoncrawl_support::paths(reader).await;
    assert_eq!(
        paths,
        [
            entry(SEGMENT, "z"),
            entry("1664030338280.510", "other"),
            entry(SEGMENT, "a")
        ]
    );
    let manifest_opens = manifest_opens.requests.lock().unwrap();
    assert_eq!(manifest_opens[0].object, request().manifest());
    assert_eq!(manifest_opens[0].options.io_concurrency.get(), 3);
    assert!(
        opens
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.options.batch_size.get() == 2
                && request.byte_options.io_concurrency.get() == 3)
    );
}

#[tokio::test]
async fn resolving_membership_does_not_open_record_files() {
    let records = Records::default();
    let opens = records.opens.clone();
    let accessor = CommonCrawlWarcAccessor::new(manifest("good.paths", 7), records);
    let resolved = accessor.resolve(request()).await.unwrap();
    assert_eq!(resolved.objects.len(), 3);
    assert!(opens.lock().unwrap().is_empty());
}

#[tokio::test]
async fn projection_and_terminal_file_errors_use_fileset_contracts() {
    let records = Records::default();
    let opens = records.opens.clone();
    let accessor = CommonCrawlWarcAccessor::new(manifest("error_sequence.paths", 7), records);
    let mut reader = accessor
        .open(request().with_options(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        }))
        .await
        .unwrap();
    assert!(reader.metadata().schema.fields().is_empty());
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(opens.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn malformed_manifests_fail_before_record_files_open() {
    for name in [
        "duplicate.paths",
        "bad_traversal.paths",
        "bad_leading_space.paths",
        "bad_absolute.paths",
        "bad_origin.paths",
        "bad_query.paths",
        "bad_encoding.paths",
        "bad_suffix.paths",
        "bad_oversized.paths",
        "bad_utf8.paths",
        "raw_manifest.paths.gz",
    ] {
        let records = Records::default();
        let opens = records.opens.clone();
        let accessor = CommonCrawlWarcAccessor::new(manifest(name, 19), records);
        assert_eq!(
            accessor.resolve(request()).await.unwrap_err().kind(),
            ErrorKind::Decode,
            "{name}"
        );
        assert!(opens.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn empty_selections_and_manifest_source_errors_are_explicit() {
    for name in ["empty.paths", "blank.paths", "other_crawl.paths"] {
        let accessor = CommonCrawlWarcAccessor::new(manifest(name, 2), Records::default());
        assert_eq!(
            accessor.resolve(request()).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    let accessor = CommonCrawlWarcAccessor::new(
        manifest("one.paths", 3).with_failure(3, ErrorKind::PermissionDenied),
        Records::default(),
    );
    assert_eq!(
        accessor.resolve(request()).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    let accessor = CommonCrawlWarcAccessor::new(
        manifest("one.paths", 1).with_empty_chunk(0),
        Records::default(),
    );
    assert_eq!(
        accessor.resolve(request()).await.unwrap_err().kind(),
        ErrorKind::Io
    );
}

#[tokio::test]
async fn caller_supplied_gzip_decodes_the_official_inventory_representation() {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
    let bytes = manifest("raw_manifest.paths.gz", 1).layer(DecompressionLayer::new(Gzip));
    let accessor = CommonCrawlWarcAccessor::new(bytes, Records::default());
    let batches = into_stream(accessor.open(request()).await.unwrap())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
}
