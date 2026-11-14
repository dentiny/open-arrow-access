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
