use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{UInt64Array, types::UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, RecordBatch, RecordMetadata, RecordReader, ScanPlanner,
    StreamReader,
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
    Box::new(StreamReader::new(
        stream::iter([Ok(batch)]),
        RecordMetadata {
            schema,
            source: Some(format!("page-{value}").into()),
        },
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
                Ok(Box::new(StreamReader::new(stream::iter(batches), metadata))
                    as Box<dyn RecordReader>)
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
