use std::sync::Arc;

use arrow_array::Int64Array;
use futures::TryStreamExt;
use http::Method;
use open_arrow_access_core::{Accessor, ReadRequest, RecordBatch, ScanPlanner, into_stream};
use open_arrow_access_file_format::parquet::ParquetAccessor;
use open_arrow_access_storage::{OpenDalAccessor, opendal};
use open_arrow_access_test_support::http::HttpFixture;
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};

fn parquet_bytes(padding: usize) -> Vec<u8> {
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as _)])
            .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "padding".into(),
            Some("x".repeat(padding)),
        )]))
        .build();
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    output
}

fn accessor(fixture: &HttpFixture) -> ParquetAccessor<OpenDalAccessor> {
    let operator =
        opendal::Operator::new(opendal_service_http::Http::default().endpoint(fixture.endpoint()))
            .unwrap()
            .with_context(fixture.context());
    ParquetAccessor::new(OpenDalAccessor::new(operator))
}

#[tokio::test]
async fn suffix_get_replaces_head_and_footer_reads_and_reuses_bytes() {
    for padding in [0, 128 * 1024] {
        let fixture = HttpFixture::new();
        fixture.file("/data.parquet", parquet_bytes(padding));
        let reader = accessor(&fixture)
            .open(ReadRequest::new("data.parquet"))
            .await
            .unwrap();
        let requests = fixture.requests();
        assert_eq!(requests.len(), if padding == 0 { 1 } else { 2 });
        assert!(requests.iter().all(|request| request.method == Method::GET));
        assert_eq!(requests[0].headers["range"], "bytes=-65536");
        let batches = into_stream(reader).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        if padding == 0 {
            assert_eq!(
                fixture.requests().len(),
                1,
                "data ranges inside the cached tail are reused too"
            );
        }
    }
}

#[tokio::test]
async fn planning_and_metadata_inspection_use_the_same_suffix_path() {
    let fixture = HttpFixture::new();
    fixture.file("/data.parquet", parquet_bytes(0));
    let accessor = accessor(&fixture);
    accessor
        .metadata(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    assert_eq!(fixture.requests().len(), 1);
    fixture.clear_requests();
    let _plan = accessor
        .plan_scan(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    assert_eq!(fixture.requests().len(), 1);
    assert_eq!(fixture.requests()[0].headers["range"], "bytes=-65536");
}

#[tokio::test]
async fn an_empty_http_object_still_reports_a_parquet_error() {
    let fixture = HttpFixture::new();
    fixture.file("/data.parquet", Vec::<u8>::new());
    let error = accessor(&fixture)
        .open(ReadRequest::new("data.parquet"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), open_arrow_access_core::ErrorKind::Decode);
    assert_eq!(fixture.requests()[0].headers["range"], "bytes=-65536");
}
