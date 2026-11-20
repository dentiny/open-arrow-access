use arrow_array::{cast::AsArray, types::UInt64Type};
use open_arrow_access_core::{Bytes, ObjectRef, RecordReader, Result, bytes_reader};
use open_arrow_access_service::arxiv::{
    ArxivSourceOptions, ArxivSourcePackaging as Packaging, open_source_package,
};
use open_arrow_access_test_support::fixture;
use std::num::NonZeroUsize;

async fn open(
    name: &str,
    packaging: Packaging,
    options: ArxivSourceOptions,
) -> Result<Box<dyn RecordReader>> {
    let input = bytes_reader(
        Bytes::from(fixture!("arxiv-articles", name).await),
        Some(ObjectRef::new("src/shard.tar#member=7")),
    );
    open_source_package(Box::new(input), "2410.00001v1", packaging, options).await
}

#[tokio::test]
async fn tar_preserves_members_and_ancillary_bytes() {
    let names = [
        "main.tex",
        "figures/a.png",
        "anc/demo.mp4",
        "anc/data.csv",
        "anc/metadata.xml",
        "anc/archive.zip",
        "main.tex",
    ];
    let mut options = ArxivSourceOptions::default();
    options.options.batch_size = NonZeroUsize::new(2).unwrap();
    options.target_bytes = NonZeroUsize::MIN;
    for (path, packaging) in [
        ("source.tar", Packaging::Tar),
        ("source.tar.gz", Packaging::GzipTar),
    ] {
        let mut reader = open(path, packaging, options.clone()).await.unwrap();
        for (index, name) in names.iter().enumerate() {
            let batch = reader.next().await.unwrap().unwrap();
            let expected = if index == 6 {
                "duplicate.tex".into()
            } else {
                format!("members/{name}")
            };
            assert_eq!(batch.num_rows(), 1); // Whole files can exceed the soft byte target.
            assert_eq!(
                batch["entry_name"].as_binary::<i64>().value(0),
                name.as_bytes()
            );
            assert_eq!(
                batch["entry_index"].as_primitive::<UInt64Type>().value(0),
                index as u64 + 1
            );
            assert_eq!(
                batch["data"].as_binary::<i64>().value(0),
                fixture!("arxiv-articles", &expected).await
            );
            assert_eq!(
                batch["article_id"].as_string::<i64>().value(0),
                "2410.00001v1"
            );
            assert_eq!(batch["format"].as_string::<i64>().value(0), "source_member");
            assert_eq!(
                batch["source_uri"].as_string::<i64>().value(0),
                "src/shard.tar#member=7"
            );
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn single_files_preserve_original_and_empty_payloads() {
    for name in ["main.tex", "paper.pdf", "empty.tex"] {
        for (path, packaging) in [
            (
                name.to_owned(),
                Packaging::Single {
                    name: name.as_bytes().to_vec(),
                },
            ),
            (
                format!("{name}.gz"),
                Packaging::GzipSingle {
                    name: name.as_bytes().to_vec(),
                },
            ),
        ] {
            let mut reader = open(&path, packaging, ArxivSourceOptions::default())
                .await
                .unwrap();
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(
                batch["data"].as_binary::<i64>().value(0),
                fixture!("arxiv-articles", name).await
            );
            assert_eq!(
                batch["entry_name"].as_binary::<i64>().value(0),
                name.as_bytes()
            );
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn rejects_invalid_gzip() {
    for name in ["bad-crc.tar.gz", "truncated.tar.gz", "trailing.tar.gz"] {
        assert!(
            open(name, Packaging::GzipTar, ArxivSourceOptions::default())
                .await
                .is_err(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn preserves_binary_names() {
    let mut reader = open(
        "binary-name.tar",
        Packaging::Tar,
        ArxivSourceOptions::default(),
    )
    .await
    .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch["entry_name"].as_binary::<i64>().value(0), b"\xff.tex");
    assert_eq!(batch["data"].as_binary::<i64>().value(0), b"abc");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn projection_preserves_rows_and_rejects_unknown_fields() {
    let mut options = ArxivSourceOptions::default();
    options.options.projection = Some(vec![]);
    let mut reader = open("source.tar", Packaging::Tar, options.clone())
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!((batch.num_columns(), batch.num_rows()), (0, 7));
    options.options.projection = Some(vec!["unknown".into()]);
    assert!(open("source.tar", Packaging::Tar, options).await.is_err());
}
