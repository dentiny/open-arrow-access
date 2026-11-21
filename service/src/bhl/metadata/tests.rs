use super::*;
use arrow_array::{Array, cast::AsArray};
use open_arrow_access_core::ErrorKind;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{fixture, temp_fs};

const ITEM: &str = "ocr/item-2/item-2-3-5.txt";
const PART: &str = "ocr/part-8/part-8-3-1.txt";
const MISSING: &str = "ocr/item-20/item-20-30-1.txt";

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
    let files = files
        .iter()
        .map(|(key, bytes)| (key.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    let (root, operator) = temp_fs(&files);
    (root, OpenDalAccessor::new(operator))
}

pub(in crate::bhl) async fn load() -> BhlMetadata {
    let (_root, storage) = source("title.txt.gz").await;
    BhlMetadata::load(storage).await.unwrap()
}

fn cell(row: &StructArray, table: &str, field: &str) -> String {
    if matches!(table, "item" | "title") {
        return list(row, table, field).remove(0);
    }
    row.column_by_name(table)
        .unwrap()
        .as_struct()
        .column_by_name(field)
        .unwrap()
        .as_string::<i64>()
        .value(0)
        .to_owned()
}

fn list(row: &StructArray, table: &str, field: &str) -> Vec<String> {
    let values = row.column_by_name(table).unwrap().as_list::<i32>().value(0);
    values
        .as_struct()
        .column_by_name(field)
        .unwrap()
        .as_string::<i64>()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect()
}

fn read(metadata: &BhlMetadata, paths: &[&str]) -> Vec<BhlPageMetadata> {
    let keys = paths
        .iter()
        .map(|path| ObjectRef::new(*path))
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    metadata
        .for_each(&keys, |row| {
            rows.push(row);
            Ok(())
        })
        .unwrap();
    rows
}

#[tokio::test]
async fn joins_pages_and_preserves_source_values_and_relation_order() {
    let metadata = load().await;
    // Multiple independent relations and duplicate creators catch multiplication
    // bugs; insertion order must survive the joins and list aggregations.
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO creator VALUES ('1', '9', '', '', '');
         INSERT INTO creator VALUES ('1', '10', 'Author', 'Second', '');
         INSERT INTO creatoridentifier VALUES ('10', '', 'other', '');
         INSERT INTO subject VALUES ('1', 'a', '');
         INSERT INTO subject VALUES ('1', 'z', '');
         INSERT INTO partpage VALUES ('8', '3', '2', '1', '');",
        )
        .unwrap();
    let rows = read(&metadata, &[ITEM, PART, MISSING]);
    assert_eq!(metadata.info.sources.len(), 13);
    assert_eq!(rows.len(), 3);
    for page in rows {
        let row = &page.metadata;
        assert_eq!(
            row.data_type(),
            page_schema()
                .field_with_name("metadata")
                .unwrap()
                .data_type()
        );
        assert!(!row.is_null(0));
        assert!(page.conflicts.is_empty());
        if page.object.locator() == MISSING {
            assert_eq!(
                page.missing,
                ["page:30", "item:20", "title:unresolved parent"]
            );
            assert!(list(row, "item", "ItemID").is_empty());
            continue;
        }
        assert!(page.missing.is_empty());
        assert_eq!(cell(row, "title", "TitleID"), "001");
        assert_eq!(cell(row, "title", "FullTitle"), "Original \"title\" 遗传");
        assert_eq!(cell(row, "title", "ShortTitle"), "");
        assert_eq!(cell(row, "item", "RightsStatement"), "Source rights");
        assert_eq!(list(row, "creator", "CreatorID"), ["9", "9", "10"]);
        assert_eq!(
            list(row, "creatoridentifier", "IdentifierValue"),
            ["same", "same", "other"]
        );
        assert_eq!(list(row, "subject", "Subject"), ["a", "z"]);
        assert_eq!(list(row, "doi", "DOI"), ["title DOI"]);
        if page.object.locator() == PART {
            assert_eq!(cell(row, "part", "RightsStatement"), "Part rights");
            assert_eq!(list(row, "partpage", "PageID"), ["3", "3"]);
        } else {
            assert!(row.column_by_name("part").unwrap().is_null(0));
            assert!(list(row, "partpage", "PageID").is_empty());
        }
    }
}

#[tokio::test]
async fn rejects_invalid_sources() {
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
async fn item_title_associations_preserve_all_records_without_multiplying_pages() {
    let (root, storage) = source("title.txt.gz").await;
    for table in ["item", "title"] {
        std::fs::write(
            root.path().join(format!("data/{table}.txt.gz")),
            fixture!("bhl-metadata", &format!("{table}-multi-title.txt.gz")).await,
        )
        .unwrap();
    }
    let metadata = BhlMetadata::load(storage).await.unwrap();
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO subject VALUES ('1', 'first subject', '');
         INSERT INTO subject VALUES ('42', 'second subject', '');
         INSERT INTO creator VALUES ('1', '9', '', '', '');
         INSERT INTO creator VALUES ('42', '10', 'Author', 'Other author', '');",
        )
        .unwrap();
    let rows = read(&metadata, &[ITEM, PART]);
    assert_eq!(rows.len(), 2);
    for page in rows {
        assert!(page.missing.is_empty());
        assert!(page.conflicts.is_empty());
        assert_eq!(list(&page.metadata, "item", "TitleID"), ["1", "42"]);
        assert_eq!(list(&page.metadata, "title", "TitleID"), ["001", "42"]);
        assert_eq!(
            list(&page.metadata, "item", "RightsStatement"),
            ["Source rights", "Source rights"]
        );
        assert_eq!(
            list(&page.metadata, "subject", "Subject"),
            ["first subject", "second subject"]
        );
        assert_eq!(
            list(&page.metadata, "creator", "CreatorID"),
            ["9", "9", "10"]
        );
    }
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch("DELETE FROM title WHERE bhl_id(TitleID) = 42")
        .unwrap();
    assert_eq!(read(&metadata, &[ITEM])[0].missing, ["title:42"]);
}

#[tokio::test]
async fn reports_conflicts_and_falls_back_to_partpage_parent() {
    let metadata = load().await;
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "UPDATE part SET ItemID = '7';
         INSERT INTO partpage VALUES ('8', '3', '9', '2', '');",
        )
        .unwrap();
    let row = read(&metadata, &[PART]).pop().unwrap();
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
    let row = read(&metadata, &[PART]).pop().unwrap();
    assert_eq!(cell(&row.metadata, "item", "ItemID"), "2");
    assert_eq!(row.missing, ["part:8"]);
}

#[tokio::test]
async fn emits_rows_until_callback_fails() {
    let metadata = load().await;
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
    let mut emitted = 0;
    let error = metadata
        .for_each(&[ITEM, PART].map(ObjectRef::new), |_| {
            emitted += 1;
            Err(error::invalid("stop sink"))
        })
        .unwrap_err();
    assert_eq!(error.to_string(), "bhl_metadata: stop sink");
    assert_eq!(emitted, 1);
    // Subsequent queries are independent, and duplicate requests stay duplicated.
    assert_eq!(read(&metadata, &[ITEM, PART]).len(), 2);
    assert_eq!(read(&metadata, &[ITEM, ITEM]).len(), 2);
    assert!(metadata.for_each(&[], |_| panic!("empty query")).is_ok());
    assert!(
        metadata
            .for_each(&[ObjectRef::new("invalid")], |_| panic!("invalid key"))
            .is_err()
    );
}

#[tokio::test]
async fn snapshots_reopen_read_only_without_source_access_and_keep_provenance() {
    let (root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage.clone()).await.unwrap();
    let expected = read(&metadata, &[ITEM, PART]);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshot'quoted.duckdb");
    BhlMetadata::prepare_snapshot(storage.clone(), &path)
        .await
        .unwrap();
    assert!(BhlMetadata::prepare_snapshot(storage, &path).await.is_err());
    let fingerprint = metadata.info.fingerprint.clone();
    drop(metadata);
    drop(root); // Reopening and queries cannot fall back to the TSV inputs.
    let snapshot = BhlMetadata::open_snapshot(&path).unwrap();
    assert_eq!(snapshot.info.fingerprint, fingerprint);
    let other = BhlMetadata::open_snapshot(&path).unwrap();
    assert_eq!(read(&other, &[ITEM]).len(), 1);
    drop(other);
    let actual = read(&snapshot, &[ITEM, PART]);
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(actual.metadata, expected.metadata);
        assert_eq!(actual.missing, expected.missing);
        assert_eq!(actual.conflicts, expected.conflicts);
    }
    assert!(
        snapshot
            .connection
            .lock()
            .unwrap()
            .execute_batch("DELETE FROM main.item")
            .is_err()
    );
    assert_eq!(read(&snapshot, &[ITEM, ITEM]).len(), 2);
    drop(snapshot);
    {
        let db = duckdb::Connection::open(&path).unwrap();
        db.execute_batch("UPDATE __bhl_snapshot SET version = 999")
            .unwrap();
    }
    assert!(BhlMetadata::open_snapshot(&path).is_err());
}
