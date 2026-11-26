use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures::{StreamExt, TryStreamExt};
use http::{Method, StatusCode, header};
use open_arrow_access_core::{
    AccessorExt, ReadRequest, ScanPlanner, into_stream, metrics::MetricsLayer,
};
use open_arrow_access_file_format::parquet::ParquetAccessor;
use open_arrow_access_storage::{OpenDalAccessor, opendal};
use open_arrow_access_test_support::{
    fixture,
    http::{HttpFixture, file_response, response},
};

fn storage(fixture: &HttpFixture) -> OpenDalAccessor {
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(fixture.endpoint()))
            .unwrap()
            .with_context(fixture.context());
    OpenDalAccessor::new(operator)
}

async fn server(etag: Option<&'static str>) -> (HttpFixture, Arc<AtomicUsize>) {
    let fixture = HttpFixture::new();
    let bytes = fixture!("parquet", "ordered-snappy.parquet").await;
    let generation = Arc::new(AtomicUsize::new(0));
    let current = generation.clone();
    fixture.handle(move |request| {
        let etag = if current.load(Ordering::SeqCst) == 0 {
            etag
        } else {
            Some("\"changed\"")
        };
        if let Some(expected) = request.headers().get(header::IF_MATCH)
            && Some(expected.to_str().unwrap()) != etag
        {
            return Ok(response(StatusCode::PRECONDITION_FAILED, "object changed"));
        }
        let mut response = file_response(&request, bytes.clone());
        if let Some(etag) = etag {
            response
                .headers_mut()
                .insert(header::ETAG, etag.parse().unwrap());
        }
        Ok(response)
    });
    (fixture, generation)
}

#[tokio::test]
async fn row_groups_skip_stat_and_bind_gets_to_the_planned_etag() {
    let (fixture, _) = server(Some("\"original\"")).await;
    // Both wrappers must forward the metadata-reuse opening hook.
    let bytes = Arc::new(storage(&fixture).layer(MetricsLayer::default()));
    let accessor = ParquetAccessor::new(bytes);
    let mut plan = accessor
        .plan_scan(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    let mut groups = 0;
    let mut rows = 0;
    while let Some(partition) = plan.partitions.next().await {
        let reader = partition.unwrap().await.unwrap();
        let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
        rows += batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        groups += 1;
    }
    assert!(groups > 1);
    assert_eq!(rows, 7);
    let requests = fixture.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == Method::HEAD)
            .count(),
        0
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.method == Method::GET)
            .skip(1)
            .all(|request| request.headers[header::IF_MATCH] == "\"original\"")
    );
}

#[tokio::test]
async fn same_length_replacement_fails_before_emitting_rows() {
    let (fixture, generation) = server(Some("\"original\"")).await;
    let accessor = ParquetAccessor::new(storage(&fixture));
    let mut plan = accessor
        .plan_scan(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    generation.store(1, Ordering::SeqCst);
    let mut reader = plan
        .partitions
        .next()
        .await
        .unwrap()
        .unwrap()
        .await
        .unwrap();
    assert!(reader.next().await.is_err());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(
        fixture
            .requests()
            .iter()
            .filter(|request| request.method == Method::HEAD)
            .count(),
        0
    );
}

#[tokio::test]
async fn missing_or_weak_etags_retain_per_partition_stat_validation() {
    for etag in [None, Some("W/\"weak\"")] {
        let (fixture, _) = server(etag).await;
        let accessor = ParquetAccessor::new(storage(&fixture));
        let mut plan = accessor
            .plan_scan(ReadRequest::new("data.parquet"))
            .await
            .unwrap();
        let mut groups = 0;
        while let Some(partition) = plan.partitions.next().await {
            let reader = partition.unwrap().await.unwrap();
            let _ = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
            groups += 1;
        }
        let requests = fixture.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == Method::HEAD)
                .count(),
            groups
        );
        assert!(
            requests
                .iter()
                .all(|request| !request.headers.contains_key(header::IF_MATCH))
        );
    }
}

#[tokio::test]
async fn unversioned_partition_opens_still_detect_a_changed_file_length() {
    let fixture = HttpFixture::new();
    fixture.file(
        "/data.parquet",
        fixture!("parquet", "ordered-snappy.parquet").await,
    );
    let accessor = ParquetAccessor::new(storage(&fixture));
    let mut plan = accessor
        .plan_scan(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    fixture.file("/data.parquet", "shortened");
    assert!(
        plan.partitions
            .next()
            .await
            .unwrap()
            .unwrap()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn zero_column_partitions_still_validate_before_using_cached_row_counts() {
    let (fixture, generation) = server(Some("\"original\"")).await;
    let accessor = ParquetAccessor::new(storage(&fixture));
    let request = ReadRequest::new("data.parquet").with_projection(Vec::<String>::new());
    let mut plan = accessor.plan_scan(request).await.unwrap();
    generation.store(1, Ordering::SeqCst);
    assert!(
        plan.partitions
            .next()
            .await
            .unwrap()
            .unwrap()
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .requests()
            .iter()
            .filter(|request| request.method == Method::HEAD)
            .count(),
        1
    );
}
