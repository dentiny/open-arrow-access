use super::*;
use arrow_array::{Array, LargeStringArray, ListArray};
use open_arrow_access_core::{Accessor, ErrorKind};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{fixture, temp_fs};

async fn source(title: &str) -> (tempfile::TempDir, OpenDalAccessor) {
    let mut files = Vec::new();
    for table in tables() {
        let filename = if table.name == "title" {
            title.to_owned()
        } else {
            format!("{}.txt.gz", table.name)
        };
        files.push((
            format!("data/{}.txt.gz", table.name),
            fixture!("bhl-metadata", &filename).await,
        ));
    }
    let borrowed = files
        .iter()
        .map(|(key, bytes)| (key.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    let (root, operator) = temp_fs(&borrowed);
    (root, OpenDalAccessor::new(operator))
}
fn cell<'a>(metadata: &'a StructArray, table: &str, field: &str) -> &'a str {
    metadata
        .column_by_name(table)
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column_by_name(field)
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(0)
}
#[tokio::test]
async fn joins_direct_tsv_metadata_preserves_rights_and_reports_gaps() {
    let (root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    drop(root); // Selections use retained source rows and never refetch TSVs.
    assert_eq!(metadata.info.sources.len(), 13);
    assert_eq!(metadata.info.sources["title"].rows, 1);
    let keys = [
        "ocr/item-2/item-2-3-5.txt",
        "ocr/part-8/part-8-3-1.txt",
        "ocr/item-20/item-20-30-1.txt",
    ]
    .map(ObjectRef::new);
    let selected = metadata.select(&keys).unwrap();
    assert!(selected.report.conflicts.is_empty());
    assert_eq!(selected.report.missing.len(), 1);
    for key in &keys[..2] {
        let row = selected
            .get(&BhlPageIdentity::parse(key.locator()).unwrap())
            .unwrap();
        assert!(!row.is_null(0));
        assert_eq!(cell(&row, "title", "TitleID"), "001");
        assert_eq!(cell(&row, "title", "FullTitle"), "Original \"title\" 遗传");
        assert_eq!(cell(&row, "title", "ShortTitle"), "");
        assert_eq!(cell(&row, "item", "RightsStatement"), "Source rights");
        for (table, count) in [("creatoridentifier", 2), ("doi", 1)] {
            let values = row
                .column_by_name(table)
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(values.value_length(0), count);
        }
    }
    let part = selected
        .get(&BhlPageIdentity::parse(keys[1].locator()).unwrap())
        .unwrap();
    assert_eq!(cell(&part, "part", "RightsStatement"), "Part rights");
    let missing = selected
        .get(&BhlPageIdentity::parse(keys[2].locator()).unwrap())
        .unwrap();
    assert!(!missing.is_null(0));
    assert!(missing.column_by_name("item").unwrap().is_null(0));
    let conflict = [ObjectRef::new("ocr/item-2/item-2-3-99.txt")];
    assert_eq!(
        metadata.select(&conflict).unwrap().report.conflicts.len(),
        1
    );
    assert!(
        selected
            .get(&BhlPageIdentity::parse(conflict[0].locator()).unwrap())
            .is_err()
    );
    assert_eq!(
        selected.schema_metadata().unwrap()["bhl.metadata.fingerprint"],
        metadata.info.fingerprint
    );
}

#[tokio::test]
async fn rejects_duplicate_numeric_singletons_changed_headers_and_missing_tables() {
    for title in ["title-duplicate.txt.gz", "title-bad-header.txt.gz"] {
        let (_root, storage) = source(title).await;
        assert!(BhlMetadata::load(storage).await.is_err(), "{title}");
    }
    let (_root, operator) = temp_fs(&[]);
    let error = BhlMetadata::load(OpenDalAccessor::new(operator))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn assigns_joined_values_to_page_records() {
    use crate::{FileSetRequest, bhl::BhlPageAccessor};
    use open_arrow_access_test_support::MemoryAccessor;
    let (_root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    let key = "ocr/item-2/item-2-3-5.txt";
    let selected = metadata.select(&[ObjectRef::new(key)]).unwrap();
    let provenance = selected.schema_metadata().unwrap();
    let pages = BhlPageAccessor::new(
        MemoryAccessor::new(b"original OCR", 64),
        "fixture-aws",
        move |id| selected.get(id),
    )
    .unwrap()
    .with_schema_metadata(provenance);
    let mut reader = pages.open(FileSetRequest::new([key])).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    let metadata = batch["metadata"]
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        cell(metadata, "title", "FullTitle"),
        "Original \"title\" 遗传"
    );
    assert_eq!(cell(metadata, "item", "RightsStatement"), "Source rights");
    assert!(reader.next().await.unwrap().is_none());
}
