use arrow_array::cast::AsArray;
use open_arrow_access_core::{Accessor, ReadOptions, ReadRequest};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_file_format::atif::{AtifAccessor, AtifFormat};
use open_arrow_access_storage::OpenDalAccessor;

#[tokio::test]
async fn reads_atif_jsonl_and_ordered_file_sets() {
    let root = open_arrow_access_test_support::fixture_path!("atif", "");
    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(&root));
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

    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(root));
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

#[tokio::test]
async fn parallel_atif_matches_sequential_values_and_source_context() {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_layers::ParallelReadLayer;
    use std::num::NonZeroUsize;
    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(
        open_arrow_access_test_support::fixture_path!("atif", ""),
    ));
    let files = FileSetAccessor::new(AtifAccessor::new(bytes, AtifFormat::JsonLines));
    let request = FileSetRequest::new(["trajectories.jsonl", "trajectories.jsonl"]);
    let mut sequential = files.open(request.clone()).await.unwrap();
    let mut parallel = files
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request)
        .await
        .unwrap();
    loop {
        let expected = sequential.next().await.unwrap();
        let actual = parallel.next().await.unwrap();
        assert_eq!(actual, expected);
        if expected.is_none() {
            break;
        }
    }
}

#[tokio::test]
async fn parallel_atif_composes_with_independent_gzip_readers() {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_decompression::{DecompressionLayer, gzip::Gzip};
    use open_arrow_access_layers::ParallelReadLayer;
    use std::{io::Write, num::NonZeroUsize};
    let root = tempfile::tempdir().unwrap();
    let original = std::fs::read(open_arrow_access_test_support::fixture_path!(
        "atif",
        "trajectories.jsonl"
    ))
    .unwrap();
    for name in ["a.gz", "b.gz"] {
        let file = std::fs::File::create(root.path().join(name)).unwrap();
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        encoder.write_all(&original).unwrap();
        encoder.finish().unwrap();
    }
    let bytes = OpenDalAccessor::new(open_arrow_access_test_support::fs_operator(root.path()))
        .layer(DecompressionLayer::new(Gzip));
    let files = FileSetAccessor::new(AtifAccessor::new(bytes, AtifFormat::JsonLines));
    let request = FileSetRequest::new(["b.gz", "a.gz", "b.gz"]);
    let mut sequential = files.open(request.clone()).await.unwrap();
    let mut parallel = files
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request)
        .await
        .unwrap();
    loop {
        let expected = sequential.next().await.unwrap();
        let actual = parallel.next().await.unwrap();
        assert_eq!(actual, expected);
        if expected.is_none() {
            break;
        }
    }
}
