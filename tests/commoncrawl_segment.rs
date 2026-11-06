mod commoncrawl_support;

use commoncrawl_support::{Records, entry, manifest, paths};
use open_arrow_access::{
    Accessor, ByteReadOptions, ErrorKind, ReadOptionsProvider,
    service::{CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest},
};
use std::num::NonZeroUsize;

const SEGMENT: &str = "1664030338280.51";
fn request() -> CommonCrawlSegmentRequest {
    CommonCrawlSegmentRequest::new("CC-MAIN-2022-40", SEGMENT).unwrap()
}

#[test]
fn segment_urls_preserve_source_identity_and_reject_aliases() {
    let request = CommonCrawlSegmentRequest::from_url(
        "https://mirror.example/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/",
    )
    .unwrap();
    assert_eq!(request.crawl(), "CC-MAIN-2022-40");
    assert_eq!(request.segment(), SEGMENT);
    assert_eq!(request.origin(), "https://mirror.example");
    assert_eq!(
        request.manifest().locator(),
        "crawl-data/CC-MAIN-2022-40/warc.paths.gz"
    );
    for value in [
        "https://host/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/?token=x",
        "https://user:pass@host/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/",
        "https://host/crawl-data/CC-MAIN-2022-40/x/../segments/1664030338280.51/",
        "https://host/crawl-data/CC-MAIN-2022-40/segments/%31.2/",
        "https://host/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51//",
        "https://host/crawl-data/CC-MAIN-2022-40//segments/1664030338280.51/",
        "file:///crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/",
    ] {
        assert_eq!(
            CommonCrawlSegmentRequest::from_url(value)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput,
            "{value}"
        );
    }
    for (crawl, segment) in [
        ("../CC-MAIN-2022-40", "1.2"),
        ("CC-MAIN-2022-40", "../1.2"),
        ("CC-MAIN-2022-40", "1.2.3"),
    ] {
        assert_eq!(
            CommonCrawlSegmentRequest::new(crawl, segment)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn segment_selects_exact_membership_in_order_with_forwarded_options() {
    let manifests = manifest("good.paths", 1);
    let manifest_opens = manifests.observations.clone();
    let records = Records::default();
    let opens = records.opens.clone();
    let accessor = CommonCrawlSegmentAccessor::new(manifests, records);
    let mut selection = request().with_byte_options(ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(4),
    });
    selection.read_options_mut().batch_size = NonZeroUsize::new(2).unwrap();
    selection.read_options_mut().projection = Some(vec!["path".into()]);
    let reader = accessor.open(selection).await.unwrap();
    assert_eq!(opens.lock().unwrap().len(), 1);
    assert!(reader.metadata().source.is_none());
    assert_eq!(
        paths(reader).await,
        [entry(SEGMENT, "z"), entry(SEGMENT, "a")]
    );
    let manifest_opens = manifest_opens.requests.lock().unwrap();
    assert_eq!(manifest_opens[0].object, request().manifest());
    assert_eq!(manifest_opens[0].options.io_concurrency.get(), 3);
    let opens = opens.lock().unwrap();
    assert_eq!(opens.len(), 2);
    assert!(
        opens
            .iter()
            .all(|request| request.options.batch_size.get() == 2
                && request.options.projection.as_ref().unwrap() == &["path"]
                && request.byte_options.io_concurrency.get() == 3)
    );
}

#[tokio::test]
async fn segment_resolution_ignores_duplicate_paths_outside_the_selected_segment() {
    let records = Records::default();
    let opens = records.opens.clone();
    let accessor = CommonCrawlSegmentAccessor::new(manifest("other_duplicate.paths", 3), records);
    let resolved = accessor.resolve(request()).await.unwrap();
    assert_eq!(resolved.objects, vec![entry(SEGMENT, "a").into()]);
    assert!(opens.lock().unwrap().is_empty());
}

#[tokio::test]
async fn neighboring_segments_do_not_satisfy_an_empty_selection() {
    let accessor =
        CommonCrawlSegmentAccessor::new(manifest("no_match.paths", 2), Records::default());
    assert_eq!(
        accessor.resolve(request()).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
