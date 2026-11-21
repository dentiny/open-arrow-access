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
fn collect(metadata: &BhlMetadata, paths: &[&str]) -> Vec<BhlPageMetadata> {
    let mut rows = Vec::new();
    metadata
        .for_each(
            &paths
                .iter()
                .map(|path| ObjectRef::new(*path))
                .collect::<Vec<_>>(),
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    rows
}

#[tokio::test]
async fn imports_all_tables_and_emits_fixed_schema_with_lexical_values_rights_and_gaps() {
    let (root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    drop(root); // The join only reads imported tables.
    assert_eq!(metadata.info.sources.len(), 13);
    assert_eq!(metadata.info.sources["title"].rows, 1);
    let rows = collect(
        &metadata,
        &[
            "ocr/item-2/item-2-3-5.txt",
            "ocr/part-8/part-8-3-1.txt",
            "ocr/item-20/item-20-30-1.txt",
        ],
    );
    assert_eq!(rows.len(), 3);
    for row in rows.iter().filter(|row| row.identity.page_id == 3) {
        assert!(row.missing.is_empty());
        assert!(row.conflicts.is_empty());
        assert!(!row.metadata.is_null(0));
        assert_eq!(
            row.metadata.data_type(),
            page_schema()
                .field_with_name("metadata")
                .unwrap()
                .data_type()
        );
        assert_eq!(cell(&row.metadata, "title", "TitleID"), "001");
        assert_eq!(
            cell(&row.metadata, "title", "FullTitle"),
            "Original \"title\" 遗传"
        );
        assert_eq!(cell(&row.metadata, "title", "ShortTitle"), "");
        assert_eq!(
            cell(&row.metadata, "item", "RightsStatement"),
            "Source rights"
        );
        assert_eq!(
            relation_cells(&row.metadata, "creatoridentifier", "IdentifierValue"),
            ["same", "same"]
        );
        assert_eq!(relation_cells(&row.metadata, "doi", "DOI"), ["title DOI"]);
    }
    let part = rows
        .iter()
        .find(|row| row.identity.entity_type == "part")
        .unwrap();
    assert_eq!(
        cell(&part.metadata, "part", "RightsStatement"),
        "Part rights"
    );
    let missing = rows.iter().find(|row| row.identity.page_id == 30).unwrap();
    assert!(!missing.missing.is_empty());
    assert!(!missing.metadata.is_null(0));
    assert!(missing.metadata.column_by_name("item").unwrap().is_null(0));
    assert_eq!(
        metadata.schema_metadata().unwrap()["bhl.metadata.fingerprint"],
        metadata.info.fingerprint
    );
}

#[tokio::test]
async fn rejects_duplicate_numeric_singletons_changed_headers_invalid_keys_and_missing_tables() {
    for title in [
        "title-duplicate.txt.gz",
        "title-bad-header.txt.gz",
        "title-invalid-key.txt.gz",
    ] {
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
async fn emitted_metadata_assigns_to_actual_page_records() {
    use crate::{FileSetRequest, bhl::BhlPageAccessor};
    use open_arrow_access_test_support::MemoryAccessor;
    let (_root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    let key = "ocr/item-2/item-2-3-5.txt";
    let row = collect(&metadata, &[key]).pop().unwrap();
    let pages = BhlPageAccessor::new(
        MemoryAccessor::new(b"original OCR", 64),
        "fixture-aws",
        move |id| {
            assert_eq!(id, &row.identity);
            Ok(row.metadata.clone())
        },
    )
    .unwrap()
    .with_schema_metadata(metadata.schema_metadata().unwrap());
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

fn relation_cells(metadata: &StructArray, table: &str, field: &str) -> Vec<String> {
    let list = metadata
        .column_by_name(table)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = list.value(0);
    let records = values.as_any().downcast_ref::<StructArray>().unwrap();
    records
        .column_by_name(field)
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn sql_aggregates_relations_without_multiplying_or_reordering_source_rows() {
    let (_root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO creator VALUES (1, 2, '1', '10', 'Author', 'Second', '');
         INSERT INTO creator VALUES (1, 1, '1', '9', '', '', '');
         INSERT INTO creatoridentifier VALUES (10, 2, '10', '', 'other', '');
         INSERT INTO subject VALUES (1, 2, '1', 'z', '');
         INSERT INTO subject VALUES (1, 1, '1', 'a', '');
         INSERT INTO partpage VALUES (8, 2, '8', '3', '2', '1', '');",
        )
        .unwrap();
    let row = collect(&metadata, &["ocr/part-8/part-8-3-1.txt"])
        .pop()
        .unwrap();
    assert_eq!(
        relation_cells(&row.metadata, "creator", "CreatorID"),
        ["9", "9", "10"]
    );
    assert_eq!(
        relation_cells(&row.metadata, "creatoridentifier", "IdentifierValue"),
        ["same", "same", "other"]
    );
    assert_eq!(
        relation_cells(&row.metadata, "subject", "Subject"),
        ["a", "z"]
    );
    assert_eq!(
        relation_cells(&row.metadata, "partpage", "PageID"),
        ["3", "3"]
    );
    assert!(row.missing.is_empty());
    assert!(row.conflicts.is_empty());
}

#[tokio::test]
async fn sql_reports_parent_and_partpage_sequence_conflicts_and_falls_back() {
    let (_root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "UPDATE part SET ItemID = '7';
         INSERT INTO partpage VALUES (8, 1, '8', '3', '9', '2', '');",
        )
        .unwrap();
    let path = "ocr/part-8/part-8-3-1.txt";
    let row = collect(&metadata, &[path]).pop().unwrap();
    assert_eq!(
        row.conflicts,
        [
            "page.ItemID=2, selected item=Some(7)",
            "partpage.ItemID=2, selected item=Some(7)",
            "partpage.ItemID=9, selected item=Some(7)",
            "SequenceOrder=2, filename sequence=1",
        ]
    );
    assert_eq!(row.missing, ["item:7", "title:unresolved parent"]);
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch("DELETE FROM part;")
        .unwrap();
    let row = collect(&metadata, &[path]).pop().unwrap();
    assert_eq!(cell(&row.metadata, "item", "ItemID"), "2");
    assert_eq!(row.missing, ["part:8"]);
}

#[tokio::test]
async fn disables_external_cache_and_emits_incrementally_with_independent_queries() {
    let (_root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    let cached: bool = metadata
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT current_setting('enable_external_file_cache')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!cached);
    let paths = ["ocr/item-2/item-2-3-5.txt", "ocr/part-8/part-8-3-1.txt"];
    let keys = paths.map(ObjectRef::new);
    let mut emitted = 0;
    let error = metadata
        .for_each(&keys, |_| {
            emitted += 1;
            Err(error::invalid("stop sink"))
        })
        .unwrap_err();
    assert_eq!(error.to_string(), "bhl_metadata: stop sink");
    assert_eq!(emitted, 1);
    assert_eq!(collect(&metadata, &paths).len(), 2); // A failed sink leaves no request state.
    assert_eq!(collect(&metadata, &[paths[0], paths[0]]).len(), 2);
    assert!(metadata.for_each(&[], |_| panic!("empty query")).is_ok());
    assert!(
        metadata
            .for_each(&[ObjectRef::new("invalid")], |_| panic!("invalid key"))
            .is_err()
    );
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let metadata = &metadata;
            scope.spawn(move || assert_eq!(collect(metadata, &paths).len(), 2));
        }
    });
}
