use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{UInt64Array, types::UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, OrderRequirement, RecordBatch, RecordMetadata, RecordReader,
    ScanPlanner, StreamReader,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::common::PageSetAccessor;
use open_arrow_access_test_support::record::collect_column;

fn page(value: u64) -> Box<dyn RecordReader> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "page",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![value]))],
    )
    .unwrap();
    Box::new(StreamReader::records(
        stream::iter([Ok(batch)]),
        RecordMetadata {
            schema,
            source: Some(format!("page-{value}").into()),
        },
        OrderRequirement::Reorderable,
    ))
}

async fn open_pages(
    pages: impl ScanPlanner<Request = Vec<u64>>,
    selection: Vec<u64>,
    parallel: bool,
) -> Box<dyn RecordReader> {
    if parallel {
        pages
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ))
            .open(selection)
            .await
            .unwrap()
    } else {
        pages.open(selection).await.unwrap()
    }
}

#[tokio::test]
async fn page_set_opens_lazily_and_preserves_selection_order() {
    for parallel in [false, true] {
        let opened = Arc::new(AtomicUsize::new(0));
        let observed = opened.clone();
        let pages = PageSetAccessor::new(move |index| {
            observed.fetch_add(1, Ordering::Relaxed);
            async move { Ok(page(index)) }
        });
        assert_eq!(opened.load(Ordering::Relaxed), 0);
        let reader = open_pages(pages, vec![2, 1, 2], parallel).await;
        assert_eq!(opened.load(Ordering::Relaxed), 1);
        assert_eq!(collect_column::<UInt64Type>(reader, 0).await, [2, 1, 2]);
        assert_eq!(opened.load(Ordering::Relaxed), 3);
    }
}

#[tokio::test]
async fn page_set_schema_drift_is_rejected_by_both_executors() {
    for parallel in [false, true] {
        for wrong_metadata in [false, true] {
            let pages = PageSetAccessor::new(move |index| async move {
                if index == 0 {
                    return Ok(page(0));
                }
                let expected = page(index).metadata().clone();
                let wrong = RecordMetadata {
                    schema: Arc::new(Schema::empty()),
                    source: None,
                };
                let (metadata, batches) = if wrong_metadata {
                    (wrong, vec![])
                } else {
                    (expected, vec![Ok(RecordBatch::new_empty(wrong.schema))])
                };
                Ok(Box::new(StreamReader::records(
                    stream::iter(batches),
                    metadata,
                    OrderRequirement::Reorderable,
                )) as Box<dyn RecordReader>)
            });
            let mut reader = open_pages(pages, vec![0, 1], parallel).await;
            // A later worker may fail before the first page is delivered.
            loop {
                match reader.next().await {
                    Ok(Some(_)) => {}
                    Err(error) => {
                        assert_eq!(error.kind(), ErrorKind::Decode);
                        break;
                    }
                    Ok(None) => panic!("schema drift must fail the scan"),
                }
            }
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn cloned_page_sets_share_nonclone_openers_and_keep_readers_independent() {
    let opens = Mutex::new(0usize);
    let pages = PageSetAccessor::new(move |index| {
        // Capturing a Mutex makes this callback non-Clone.
        *opens.lock().unwrap() += 1;
        async move { Ok(page(index)) }
    });
    let sequential = pages.clone().open(vec![2, 1]).await.unwrap();
    let parallel = open_pages(pages.clone(), vec![3, 2, 3], true).await;
    drop(pages);
    assert_eq!(collect_column::<UInt64Type>(parallel, 0).await, [3, 2, 3]);
    assert_eq!(collect_column::<UInt64Type>(sequential, 0).await, [2, 1]);
}

#[tokio::test]
async fn parallel_page_fetches_overlap_but_stay_within_the_resident_budget() {
    use tokio::sync::Semaphore;
    let release_first = Arc::new(Semaphore::new(0));
    let second_ready = Arc::new(Semaphore::new(0));
    let later_opened = Arc::new(AtomicUsize::new(0));
    let pages = PageSetAccessor::new({
        let release_first = release_first.clone();
        let second_ready = second_ready.clone();
        let later_opened = later_opened.clone();
        move |index| {
            let release_first = release_first.clone();
            let second_ready = second_ready.clone();
            let later_opened = later_opened.clone();
            async move {
                match index {
                    1 => {
                        release_first.acquire().await.unwrap().forget();
                    }
                    2 => second_ready.add_permits(1),
                    3 => {
                        later_opened.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {}
                }
                Ok(page(index))
            }
        }
    });
    let reader = open_pages(pages, vec![0, 1, 2, 3], true).await;
    let consumed = tokio::spawn(async move { collect_column::<UInt64Type>(reader, 0).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), second_ready.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    // Page 2 fetched while page 1 is blocked; its queued batch still owns a slot.
    assert_eq!(later_opened.load(Ordering::SeqCst), 0);
    release_first.add_permits(1);
    let values = tokio::time::timeout(std::time::Duration::from_secs(5), consumed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(values, [0, 1, 2, 3]);
    assert_eq!(later_opened.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicit_api_pages_reuse_authentication_and_do_not_follow_continuations() {
    use open_arrow_access_core::ReadRequest;
    use open_arrow_access_file_format::json::JsonFormat;
    use open_arrow_access_service::common::Connection;
    use open_arrow_access_test_support::http::HttpFixture;
    let http = HttpFixture::new();
    for number in [1, 2] {
        http.respond(
            &format!("/records?page={number}"),
            http::StatusCode::OK,
            format!(r#"{{"items":[{{"id":{number}}}],"next":"/not-discovered"}}"#),
        );
    }
    let connection = Connection::new(http.endpoint())
        .unwrap()
        .with_header("x-api-key", "fixture-secret")
        .unwrap()
        .with_context(opendal::OperationContext::new());
    let format = JsonFormat::new(Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::UInt64,
        false,
    )])));
    let pages = PageSetAccessor::new(move |number: u64| {
        let connection = connection.clone();
        let format = format.clone();
        async move {
            let document = connection
                .json("records", &[("page", &number.to_string())])
                .await?;
            document
                .records_at("/items", format)?
                .open(ReadRequest::new(format!("page-{number}")))
                .await
        }
    });
    let reader = open_pages(pages, vec![2, 1, 2], true).await;
    assert_eq!(
        http.requests().len(),
        1,
        "only the schema page opens eagerly"
    );
    assert_eq!(collect_column::<UInt64Type>(reader, 0).await, [2, 1, 2]);
    assert_eq!(http.requests().len(), 3);
    assert!(
        http.requests()
            .iter()
            .all(|request| request.headers["x-api-key"] == "fixture-secret")
    );
    assert!(
        http.requests()
            .iter()
            .all(|request| request.path.starts_with("/records?page="))
    );
}
