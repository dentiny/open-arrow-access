use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow_array::{UInt64Array, types::UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{Accessor, RecordBatch, RecordMetadata, RecordReader, StreamReader};
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

#[tokio::test]
async fn page_set_opens_lazily_and_preserves_selection_order() {
    let opened = Arc::new(AtomicUsize::new(0));
    let observed = opened.clone();
    let pages = PageSetAccessor::new(move |index| {
        observed.fetch_add(1, Ordering::Relaxed);
        async move { Ok(page(index)) }
    });
    assert_eq!(opened.load(Ordering::Relaxed), 0);
    let reader = pages.open(vec![2, 1, 2]).await.unwrap();
    assert_eq!(opened.load(Ordering::Relaxed), 1);
    assert_eq!(collect_column::<UInt64Type>(reader, 0).await, [2, 1, 2]);
    assert_eq!(opened.load(Ordering::Relaxed), 3);
}
