use open_arrow_access::{
    ErrorKind, ReadOptions, Reader, ScanPlanner,
    file_format::json::JsonFormat,
    service::{FileSetAccessor, FileSetRequest, ResolvedFileSet},
    storage::NamedFileAccessor,
};
use open_arrow_access_test_support::MemoryAccessor;

#[tokio::test]
async fn service_facade_plans_selected_attachments_without_directory_features() {
    let input = MemoryAccessor::new(b"{\"id\":1}\n", 4);
    let observations = input.observations.clone();
    let files = ResolvedFileSet::new(
        NamedFileAccessor::new(
            input,
            vec![
                ("first".into(), "a.json".into()),
                ("second".into(), "b.json".into()),
            ],
        )
        .unwrap(),
        "manifest-version",
    );
    assert_eq!(files.metadata(), &"manifest-version");
    let records: FileSetAccessor<_> = files.records(JsonFormat::infer()).with_file_planning();
    assert_eq!(
        records
            .plan_scan(FileSetRequest::new(["missing"]))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound,
    );
    assert!(observations.requests.lock().unwrap().is_empty());
    let request = FileSetRequest::new(["second", "first", "second"]).with_options(ReadOptions {
        projection: Some(vec!["id".into()]),
        ..Default::default()
    });
    let mut reader = records.plan_scan(request).await.unwrap().into_reader();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema().field(0).name(), "id");
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);
    assert_eq!(
        observations.requests.lock().unwrap()[0].object.locator(),
        "b.json"
    );
}
