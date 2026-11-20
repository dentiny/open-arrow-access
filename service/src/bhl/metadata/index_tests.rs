use super::*;
use flate2::{Compression, write::GzEncoder};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::temp_fs;
use std::io::Write;

fn files(duplicate: bool) -> Vec<(String, Vec<u8>)> {
    tables()
        .into_iter()
        .map(|table| {
            let mut text = table
                .fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join("\t")
                + "\n";
            if table.name == "title" {
                let cells = table
                    .fields
                    .iter()
                    .map(|field| match field.name().as_str() {
                        "TitleID" => "001",
                        "FullTitle" => "Original \"title\" 遗传",
                        _ => "",
                    })
                    .collect::<Vec<_>>();
                text += &(cells.join("\t") + "\n");
                if duplicate {
                    text += &format!("1{}\n", "\t".repeat(table.fields.len() - 1));
                }
            }
            let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
            gzip.write_all(text.as_bytes()).unwrap();
            (
                format!("data/{}.txt.gz", table.name),
                gzip.finish().unwrap(),
            )
        })
        .collect()
}
#[tokio::test]
async fn snapshot_preserves_cells_and_queries_without_source_files() {
    let files = files(false);
    let borrowed = files
        .iter()
        .map(|(p, b)| (p.as_str(), b.as_slice()))
        .collect::<Vec<_>>();
    let (source, operator) = temp_fs(&borrowed);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("metadata.sqlite");
    let info = prepare(OpenDalAccessor::new(operator), "s3://bhl-open-data/", &path)
        .await
        .unwrap();
    drop(source);
    assert_eq!(inspect(&path).unwrap().fingerprint, info.fingerprint);
    assert_eq!(info.sources.len(), 13);
    assert_eq!(info.sources["title"].rows, 1);
    let mut index = BhlMetadataIndex::open(&path).unwrap();
    let rows = index.rows("title", Some(1)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ordinal, 0);
    assert_eq!(rows[0].values["TitleID"], "001");
    assert_eq!(rows[0].values["FullTitle"], "Original \"title\" 遗传");
    assert_eq!(rows[0].values["ShortTitle"], "");
    assert!(index.rows("title", Some(2)).unwrap().is_empty());
    assert!(index.rows("unknown", Some(1)).is_err());
}
#[tokio::test]
async fn duplicate_numeric_singletons_never_publish_a_snapshot() {
    let files = files(true);
    let borrowed = files
        .iter()
        .map(|(p, b)| (p.as_str(), b.as_slice()))
        .collect::<Vec<_>>();
    let (_source, operator) = temp_fs(&borrowed);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("metadata.sqlite");
    assert!(
        prepare(OpenDalAccessor::new(operator), "s3://bhl-open-data/", &path)
            .await
            .is_err()
    );
    assert!(!path.exists());
}
#[tokio::test]
async fn missing_source_table_never_publishes_a_snapshot() {
    let (_source, operator) = temp_fs(&[]);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("incomplete.sqlite");
    assert!(
        prepare(OpenDalAccessor::new(operator), "s3://bhl-open-data/", &path)
            .await
            .is_err()
    );
    assert!(!path.exists());
    assert!(inspect(&path.with_extension("building.sqlite")).is_err());
}
