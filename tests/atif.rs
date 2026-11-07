use arrow_array::cast::AsArray;
use open_arrow_access::{
    Accessor, ReadOptions, ReadRequest,
    dataset::{FileSetAccessor, FileSetRequest},
    file_format::{AtifAccessor, AtifFormat},
    storage::OpenDalAccessor,
};

#[tokio::test]
async fn facade_reads_atif_jsonl_and_ordered_file_sets() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/atif");
    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(&root).await);
    let mut reader = AtifAccessor::new(bytes, AtifFormat::JsonLines)
        .open(ReadRequest::new("trajectories.jsonl"))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.schema(), reader.metadata().schema);
    assert_eq!(
        batch["source_uri"].as_string::<i32>().value(0),
        "trajectories.jsonl"
    );
    assert!(reader.next().await.unwrap().is_none());

    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(root).await);
    let files = FileSetAccessor::new(AtifAccessor::new(bytes, AtifFormat::Json));
    let mut reader = files
        .open(
            FileSetRequest::new(["trajectory.json", "trajectory.json"]).with_options(ReadOptions {
                projection: Some(vec!["trajectory_id".into(), "source_record_index".into()]),
                ..ReadOptions::default()
            }),
        )
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), reader.metadata().schema);
        assert_eq!(batch["trajectory_id"].as_string::<i32>().value(0), "first");
        assert_eq!(
            batch["source_record_index"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(0),
            1
        );
        rows += batch.num_rows();
    }
    assert_eq!(rows, 2);
}
