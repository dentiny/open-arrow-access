use arrow_array::cast::AsArray;
use open_arrow_access_core::{ByteReader, Bytes, ObjectRef, ReadOptions, bytes_reader};
use open_arrow_access_service::arxiv::{
    ArxivSourceOptions, ArxivSourcePackaging as Packaging, open_source_package,
};
use open_arrow_access_test_support::fixture;
use std::num::NonZeroUsize;

fn input(data: Vec<u8>) -> Box<dyn ByteReader> {
    Box::new(bytes_reader(
        Bytes::from(data),
        Some(ObjectRef::new("src/shard.tar#member=7")),
    ))
}
async fn load(name: &str) -> Vec<u8> {
    fixture!("arxiv-articles", name).await
}

#[tokio::test]
async fn expands_tex_figures_video_and_data_preserving_duplicates_indices_and_bytes() {
    let mut members = Vec::new();
    for name in [
        "main.tex",
        "figures/a.png",
        "anc/demo.mp4",
        "anc/data.csv",
        "anc/metadata.xml",
        "anc/archive.zip",
    ] {
        members.push((name, load(&format!("members/{name}")).await));
    }
    members.push(("main.tex", load("duplicate.tex").await));
    let mut options = ArxivSourceOptions::default();
    options.options.batch_size = NonZeroUsize::new(2).unwrap();
    options.target_bytes = NonZeroUsize::new(1).unwrap();
    for packaging in [Packaging::Tar, Packaging::GzipTar] {
        let data = load(if matches!(packaging, Packaging::GzipTar) {
            "source.tar.gz"
        } else {
            "source.tar"
        })
        .await;
        let mut reader =
            open_source_package(input(data), "2410.00001v2", packaging, options.clone())
                .await
                .unwrap();
        let mut index = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.num_rows(), 1); // soft byte target: whole members survive
            assert_eq!(
                batch
                    .column_by_name("entry_name")
                    .unwrap()
                    .as_binary::<i64>()
                    .value(0),
                members[index].0.as_bytes()
            );
            assert_eq!(
                batch
                    .column_by_name("entry_index")
                    .unwrap()
                    .as_primitive::<arrow_array::types::UInt64Type>()
                    .value(0),
                index as u64 + 1
            ); // directory skipped
            assert_eq!(
                batch
                    .column_by_name("data")
                    .unwrap()
                    .as_binary::<i64>()
                    .value(0),
                members[index].1
            );
            assert_eq!(
                batch
                    .column_by_name("format")
                    .unwrap()
                    .as_string::<i64>()
                    .value(0),
                "source_member"
            );
            assert_eq!(
                batch
                    .column_by_name("source_uri")
                    .unwrap()
                    .as_string::<i64>()
                    .value(0),
                "src/shard.tar#member=7"
            );
            index += 1;
        }
        assert_eq!(index, members.len());
    }
}

#[tokio::test]
async fn gzip_single_sources_and_empty_files_are_not_assumed_to_be_archives() {
    for name in ["main.tex", "paper.pdf", "empty.tex"] {
        let original = load(name).await;
        for compressed in [false, true] {
            let packaging = if compressed {
                Packaging::GzipSingle {
                    name: name.as_bytes().to_vec(),
                }
            } else {
                Packaging::Single {
                    name: name.as_bytes().to_vec(),
                }
            };
            let path = if compressed {
                format!("{name}.gz")
            } else {
                name.into()
            };
            let mut reader = open_source_package(
                input(load(&path).await),
                "2410.00001v1",
                packaging,
                ArxivSourceOptions::default(),
            )
            .await
            .unwrap();
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch["data"].as_binary::<i64>().value(0), original);
            assert_eq!(
                batch["entry_name"].as_binary::<i64>().value(0),
                name.as_bytes()
            );
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn rejects_gzip_corruption_truncation_trailing_bytes_and_decoded_bombs() {
    for name in ["bad-crc.tar.gz", "truncated.tar.gz", "trailing.tar.gz"] {
        assert!(
            open_source_package(
                input(load(name).await),
                "2410.00001",
                Packaging::GzipTar,
                ArxivSourceOptions::default()
            )
            .await
            .is_err()
        );
    }
    let valid = load("source.tar.gz").await;
    let options = ArxivSourceOptions {
        max_decoded_bytes: NonZeroUsize::new(10).unwrap(),
        ..Default::default()
    };
    assert!(
        open_source_package(input(valid), "2410.00001", Packaging::GzipTar, options)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn applies_tar_limits_and_preserves_non_utf8_names_without_filesystem_extraction() {
    let data = load("binary-name.tar").await;
    let mut reader = open_source_package(
        input(data.clone()),
        "2410.00001",
        Packaging::Tar,
        ArxivSourceOptions::default(),
    )
    .await
    .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        batch
            .column_by_name("entry_name")
            .unwrap()
            .as_binary::<i64>()
            .value(0),
        b"\xff.tex"
    );
    let mut options = ArxivSourceOptions::default();
    options.tar_limits.max_entry_bytes = 2;
    let mut reader = open_source_package(input(data), "2410.00001", Packaging::Tar, options)
        .await
        .unwrap();
    assert!(reader.next().await.is_err());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn source_projection_retains_row_count_and_rejects_unknown_fields() {
    let data = load("source.tar").await;
    let mut options = ArxivSourceOptions {
        options: ReadOptions::default(),
        ..Default::default()
    };
    options.options.projection = Some(vec![]);
    let mut reader = open_source_package(
        input(data.clone()),
        "2410.00001",
        Packaging::Tar,
        options.clone(),
    )
    .await
    .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!((batch.num_columns(), batch.num_rows()), (0, 7));
    options.options.projection = Some(vec!["unknown".into()]);
    assert!(
        open_source_package(input(data), "2410.00001", Packaging::Tar, options)
            .await
            .is_err()
    );
}
