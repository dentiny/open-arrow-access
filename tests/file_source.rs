use open_arrow_access::{
    OuterCompression, ReadRequest, file_format::json::JsonFormat, open_file_source,
};
use open_arrow_access_test_support::{fixture_path, fs_operator};

#[tokio::test]
async fn opens_a_file_source_and_reads_it_again() {
    let path = fixture_path!("json", "good.json");
    let source = open_file_source(
        fs_operator(path.parent().unwrap()),
        ReadRequest::new("good.json"),
        JsonFormat::infer(),
        OuterCompression::None,
    )
    .await
    .unwrap();

    for _ in 0..2 {
        let mut reader = source.reader().await.unwrap();
        assert_eq!(reader.metadata().schema, source.schema());
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 4);
    }
}

#[tokio::test]
async fn schema_drift_preserves_file_source_error_context() {
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir(std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = TempDir(std::env::temp_dir().join(format!(
        "open-arrow-access-file-source-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
    )));
    std::fs::create_dir_all(&root.0).unwrap();
    let operator = fs_operator(&root.0);
    operator.write("data.json", "{\"id\":1}\n").await.unwrap();
    let source = open_file_source(
        operator.clone(),
        ReadRequest::new("data.json"),
        JsonFormat::infer(),
        OuterCompression::None,
    )
    .await
    .unwrap();
    drop(source.reader().await.unwrap());
    operator
        .write("data.json", "{\"title\":\"changed\"}\n")
        .await
        .unwrap();
    let error = source.reader().await.err().unwrap();
    assert_eq!(error.kind(), open_arrow_access::ErrorKind::Decode);
    assert_eq!(error.operation(), "file_source_reader");
    assert!(
        error
            .to_string()
            .contains("file schema differs from the schema fixed at open")
    );
}
