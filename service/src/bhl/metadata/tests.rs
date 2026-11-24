use super::*;
use arrow_array::{Array, cast::AsArray};
use open_arrow_access_core::ErrorKind;
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{fixture, temp_fs};

const ITEM: &str = "ocr/item-2/item-2-3-5.txt";
const PART: &str = "ocr/part-8/part-8-3-1.txt";
const MISSING: &str = "ocr/item-20/item-20-30-1.txt";

#[tokio::test]
async fn batched_join_preserves_nonadjacent_versions_and_request_order() {
    let metadata = load_with_sql("").await;
    let paths = ["ocr/item-2/item-2-3-999.txt", PART, ITEM];
    let expected = read(&metadata, &paths);
    for batch_size in [1, 2] {
        let connection = metadata.query_connection().unwrap();
        let mut rows = Vec::new();
        join::for_each_batched(
            &connection,
            &paths.map(ObjectRef::new),
            batch_size,
            |page| {
                rows.push(page);
                Ok(())
            },
        )
        .unwrap();
        assert_pages_eq(&rows, &expected);
        assert_eq!(
            rows.iter()
                .map(|row| row.object.locator())
                .collect::<Vec<_>>(),
            [PART, ITEM]
        );
    }
}

#[tokio::test]
async fn batched_join_rejects_duplicate_authoritative_keys_across_batches() {
    let metadata = load_with_sql("").await;
    let connection = metadata.query_connection().unwrap();
    let mut emitted = 0;
    let result = join::for_each_batched(
        &connection,
        &[ITEM, PART, ITEM].map(ObjectRef::new),
        1,
        |_| {
            emitted += 1;
            Ok(())
        },
    );
    assert!(result.is_err());
    assert_eq!(emitted, 0);
}

#[tokio::test]
async fn prepared_lookup_preserves_lexical_values_relation_order_and_read_only_queries() {
    let (_directory, path, expected) = plain_database().await;
    BhlMetadata::prepare_lookup(&path).await.unwrap();
    let before = tokio::fs::read(&path).await.unwrap();
    let metadata = BhlMetadata::open_tables(&path, "prepared-v1").unwrap();
    let actual = read(&metadata, &[ITEM, PART]);
    assert_pages_eq(&actual, &expected);
    let connection = metadata.query_connection().unwrap();
    let raw: String = connection
        .query_row("SELECT TitleID FROM main.title LIMIT 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(raw, "001");
    let indexes: u64 = connection
        .query_row(
            "SELECT count(*) FROM duckdb_indexes() WHERE index_name LIKE '__bhl_lookup_%_key'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(indexes, 13);
    drop(connection);
    drop(metadata);
    assert_eq!(tokio::fs::read(&path).await.unwrap(), before);
    assert!(BhlMetadata::prepare_lookup(&path).await.is_err());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), before);
}

#[tokio::test]
async fn prepared_lookup_rejects_incomplete_or_unknown_profiles_without_fallback() {
    for sql in [
        "UPDATE __bhl_lookup SET version=2",
        "DROP TABLE __bhl_lookup_page",
        "DROP INDEX __bhl_lookup_page_key; ALTER TABLE __bhl_lookup_page ALTER __key TYPE VARCHAR",
        "DELETE FROM __bhl_lookup_page",
    ] {
        let (_directory, path, _) = plain_database().await;
        BhlMetadata::prepare_lookup(&path).await.unwrap();
        update_database(&path, sql);
        assert!(BhlMetadata::open_tables(&path, "invalid").is_err(), "{sql}");
    }
    let directory = tempfile::tempdir().unwrap();
    assert!(BhlMetadata::prepare_lookup(directory.path()).await.is_err());
    let missing = directory.path().join("missing.duckdb");
    assert!(BhlMetadata::prepare_lookup(&missing).await.is_err());
    assert!(!tokio::fs::try_exists(&missing).await.unwrap());
}

#[tokio::test]
async fn scoped_join_preserves_full_results_for_items_parts_and_missing_targets() {
    let metadata = load_with_sql(
        "INSERT INTO page SELECT * REPLACE ('999999' AS PageID) FROM page LIMIT 1;
         INSERT INTO pagename VALUES ('99','unrelated','999999','');
         INSERT INTO creator VALUES ('1','10','Author','second','');
         INSERT INTO creatoridentifier VALUES ('10','other','identifier','');
         INSERT INTO doi VALUES ('PAGE','3','second page DOI','');
         INSERT INTO doi VALUES ('item','20','missing item DOI','');",
    )
    .await;
    let connection = metadata.query_connection().unwrap();
    connection.execute_batch(
        "CREATE TEMP TABLE bhl_selection AS
         SELECT 0::UBIGINT request_id,'item' entity_type,2::UBIGINT entity_id,3::UBIGINT page_id,5::UBIGINT AS \"sequence\"
         UNION ALL SELECT 1,'part',8,3,1
         UNION ALL SELECT 2,'item',20,30,1;"
    ).unwrap();
    let sql = format!(
        "SELECT count(*), bit_xor(hash(request_id,metadata,missing,conflicts,canonical_id,canonical_matches)) FROM ({})",
        include_str!("join.sql")
    );
    let checksum = || {
        connection
            .query_row(&sql, [], |row| {
                Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?))
            })
            .unwrap()
    };
    let original = checksum();
    scope::prepare(&connection).unwrap();
    assert_eq!(checksum(), original);
    assert_eq!(original.0, 3);
    let unrelated: u64 = connection
        .query_row(
            "SELECT count(*) FROM bhl_page WHERE __key=999999",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(unrelated, 0);
    let source_rows: u64 = connection
        .query_row(
            "SELECT count(*) FROM main.page WHERE bhl_id(PageID)=999999",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(source_rows, 1);
}

async fn source(title: &str) -> (tempfile::TempDir, OpenDalAccessor) {
    source_table("title", title).await
}

async fn source_table(name: &str, filename: &str) -> (tempfile::TempDir, OpenDalAccessor) {
    let mut files = Vec::new();
    for table in tables() {
        let filename = if table.name == name {
            filename.to_owned()
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

pub(in crate::bhl) async fn load_with_sql(sql: &str) -> BhlMetadata {
    let metadata = load().await;
    execute(&metadata, sql);
    metadata
}

fn execute(metadata: &BhlMetadata, sql: &str) {
    metadata
        .connection
        .lock()
        .unwrap()
        .execute_batch(sql)
        .unwrap();
}

fn update_database(path: &std::path::Path, sql: &str) {
    let connection = ::duckdb::Connection::open(path).unwrap();
    connection.execute_batch(sql).unwrap();
    connection.close().unwrap();
}

fn assert_pages_eq(actual: &[BhlPageMetadata], expected: &[BhlPageMetadata]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.metadata, expected.metadata);
        assert_eq!(actual.missing, expected.missing);
        assert_eq!(actual.conflicts, expected.conflicts);
    }
}

fn cell(row: &StructArray, table: &str, field: &str) -> String {
    if matches!(table, "item" | "title" | "page" | "part") {
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

fn invalid_selection(metadata: &BhlMetadata, paths: &[&str]) {
    let keys = paths
        .iter()
        .map(|path| ObjectRef::new(*path))
        .collect::<Vec<_>>();
    let error = metadata.for_each(&keys, |_| Ok(())).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("SequenceOrder"), "{error}");
}

#[tokio::test]
async fn joins_pages_and_preserves_source_values_and_relation_order() {
    let metadata = load().await;
    // Multiple independent relations and duplicate creators catch multiplication
    // bugs; insertion order must survive the joins and list aggregations.
    execute(
        &metadata,
        "INSERT INTO creator VALUES ('1', '9', '', '', '');
         INSERT INTO creator VALUES ('1', '10', 'Author', 'Second', '');
         INSERT INTO creatoridentifier VALUES ('10', '', 'other', '');
         INSERT INTO subject VALUES ('1', 'a', '');
         INSERT INTO subject VALUES ('1', 'z', '');
         INSERT INTO partpage VALUES ('8', '3', '2', '1', '');
         INSERT INTO doi VALUES ('PAGE', '3', 'page DOI', '');
         INSERT INTO doi VALUES ('item', '2', 'item DOI', '');
         INSERT INTO doi VALUES ('page', '3', 'page DOI', '');
         INSERT INTO doi VALUES ('part', '8', 'part DOI', '');
         INSERT INTO doi VALUES ('title', '2', 'unrelated DOI', '');",
    );
    let rows = read(&metadata, &[ITEM, PART]);
    assert_eq!(metadata.info.sources.len(), 13);
    assert_eq!(rows.len(), 2);
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
        let mut expected_dois = vec!["title DOI", "page DOI", "item DOI", "page DOI"];
        if page.object.locator() == PART {
            expected_dois.push("part DOI");
        }
        assert_eq!(list(row, "doi", "DOI"), expected_dois);
        if page.object.locator() == PART {
            assert_eq!(cell(row, "part", "RightsStatement"), "Part rights");
            assert_eq!(list(row, "partpage", "PageID"), ["3", "3"]);
        } else {
            assert!(list(row, "part", "PartID").is_empty());
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
        tokio::fs::write(
            root.path().join(format!("data/{table}.txt.gz")),
            fixture!("bhl-metadata", &format!("{table}-multi-title.txt.gz")).await,
        )
        .await
        .unwrap();
    }
    let metadata = BhlMetadata::load(storage).await.unwrap();
    execute(
        &metadata,
        "INSERT INTO subject VALUES ('1', 'first subject', '');
         INSERT INTO subject VALUES ('42', 'second subject', '');
         INSERT INTO creator VALUES ('1', '9', '', '', '');
         INSERT INTO creator VALUES ('42', '10', 'Author', 'Other author', '');
         INSERT INTO doi VALUES ('Title', '42', 'second title DOI', '');",
    );
    let rows = read(&metadata, &[ITEM, PART]);
    assert_eq!(rows.len(), 2);
    for page in rows {
        assert!(page.missing.is_empty());
        assert!(page.conflicts.is_empty());
        assert_eq!(list(&page.metadata, "item", "TitleID"), ["1", "42"]);
        assert_eq!(list(&page.metadata, "title", "TitleID"), ["001", "42"]);
        assert_eq!(
            list(&page.metadata, "doi", "DOI"),
            ["title DOI", "second title DOI"]
        );
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
    execute(
        &metadata,
        "INSERT INTO item SELECT * FROM item WHERE bhl_id(TitleID) = 1",
    );
    assert_eq!(
        list(&read(&metadata, &[ITEM])[0].metadata, "doi", "DOI"),
        ["title DOI", "second title DOI"]
    );
    execute(&metadata, "DELETE FROM title WHERE bhl_id(TitleID) = 42");
    assert_eq!(read(&metadata, &[ITEM])[0].missing, ["title:42"]);
}

#[tokio::test]
async fn reports_conflicts_and_falls_back_to_partpage_parent() {
    let metadata = load().await;
    execute(
        &metadata,
        "UPDATE part SET ItemID = '7';
         INSERT INTO partpage VALUES ('8', '3', '9', '2', '');",
    );
    invalid_selection(&metadata, &[PART]);
    execute(&metadata, "DELETE FROM partpage WHERE SequenceOrder = '2'");
    let row = read(&metadata, &[PART]).pop().unwrap();
    assert_eq!(
        row.conflicts,
        [
            "page.ItemID=2, selected item=Some(7)",
            "partpage.ItemID=2, selected item=Some(7)",
        ]
    );
    assert_eq!(row.missing, ["item:7", "title:unresolved parent"]);
    execute(&metadata, "DELETE FROM part;");
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
    // Subsequent queries are independent; missing/ambiguous selections fail.
    assert_eq!(read(&metadata, &[ITEM, PART]).len(), 2);
    invalid_selection(&metadata, &[ITEM, ITEM]);
    invalid_selection(&metadata, &[MISSING]);
    assert!(metadata.for_each(&[], |_| panic!("empty query")).is_ok());
    assert!(
        metadata
            .for_each(&[ObjectRef::new("invalid")], |_| panic!("invalid key"))
            .is_err()
    );
}

#[tokio::test]
async fn databases_reopen_without_source_access_and_keep_provenance() {
    let (root, storage) = source("title.txt.gz").await;
    let metadata = BhlMetadata::load(storage.clone()).await.unwrap();
    let expected = read(&metadata, &[ITEM, PART]);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database'quoted.duckdb");
    BhlMetadata::create(storage.clone(), &path).await.unwrap();
    assert!(BhlMetadata::create(storage, &path).await.is_err());
    let fingerprint = metadata.info.fingerprint.clone();
    drop(metadata);
    drop(root); // Reopening and queries cannot fall back to the TSV inputs.
    let database = BhlMetadata::open(&path).unwrap();
    let stored_views: u64 = database
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT count(*) FROM duckdb_views() WHERE database_name=current_database()
             AND NOT temporary AND view_name LIKE 'bhl_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_views, 0); // Query helpers are no longer persisted at import.
    assert_eq!(database.info.fingerprint, fingerprint);
    let other = BhlMetadata::open(&path).unwrap();
    assert_eq!(read(&other, &[ITEM]).len(), 1);
    drop(other);
    let actual = read(&database, &[ITEM, PART]);
    assert_pages_eq(&actual, &expected);
    assert!(
        database
            .connection
            .lock()
            .unwrap()
            .execute_batch("DELETE FROM main.item")
            .is_err()
    );
    invalid_selection(&database, &[ITEM, ITEM]);
    drop(database);
    update_database(&path, "UPDATE __bhl_metadata SET version = 999");
    assert!(BhlMetadata::open(&path).is_err());
}

#[tokio::test]
async fn failed_import_does_not_produce_a_readable_database() {
    let (_root, storage) = source("title-invalid-key.txt.gz").await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("incomplete.duckdb");
    assert!(BhlMetadata::create(storage, &path).await.is_err());
    assert!(BhlMetadata::open(&path).is_err());
}

async fn plain_database() -> (tempfile::TempDir, std::path::PathBuf, Vec<BhlPageMetadata>) {
    let metadata = load().await;
    let expected = read(&metadata, &[ITEM, PART]);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("plain'quoted.duckdb");
    let connection = metadata.connection.lock().unwrap();
    connection
        .execute_batch(&format!(
            "ATTACH '{}' AS plain_dump;",
            path.to_string_lossy().replace('\'', "''")
        ))
        .unwrap();
    for table in tables() {
        connection
            .execute_batch(&format!(
                "CREATE TABLE plain_dump.main.\"{}\" AS SELECT * FROM main.\"{}\";",
                table.name, table.name
            ))
            .unwrap();
    }
    connection.execute_batch("DETACH plain_dump;").unwrap();
    drop(connection);
    drop(metadata);
    (directory, path, expected)
}

#[tokio::test]
async fn plain_and_legacy_tables_keep_read_only_queries_isolated() {
    for setup in [
        // Unrelated NULL cells must not affect the selected pages.
        "INSERT INTO title SELECT * REPLACE ('999999' AS TitleID, NULL AS FullTitle) FROM title LIMIT 1;",
        // Temporary helpers must shadow obsolete persistent definitions.
        "CREATE MACRO bhl_id(value) AS 0::UBIGINT;
         CREATE VIEW bhl_page AS SELECT 0::UBIGINT AS __key; CHECKPOINT;",
    ] {
        let (_directory, path, expected) = plain_database().await;
        update_database(&path, setup);
        let original = tokio::fs::read(&path).await.unwrap();
        // Managed databases retain their strict importer-manifest contract.
        assert!(BhlMetadata::open(&path).is_err());
        let metadata = Arc::new(BhlMetadata::open_tables(&path, "fixture-database-v1").unwrap());
        assert_eq!(metadata.info.fingerprint, "fixture-database-v1");
        assert!(metadata.info.sources.is_empty());
        let actual = read(&metadata, &[ITEM, PART]);
        assert_pages_eq(&actual, &expected);
        // Each clone must initialize its own temporary helpers and selection table.
        let readers = [ITEM, PART].map(|key| {
            let metadata = metadata.clone();
            std::thread::spawn(move || {
                assert_eq!(read(&metadata, &[key]).len(), 1);
                assert_eq!(read(&metadata, &[key]).len(), 1);
            })
        });
        for reader in readers {
            reader.join().unwrap();
        }
        assert!(
            metadata
                .connection
                .lock()
                .unwrap()
                .execute_batch("DELETE FROM main.page")
                .is_err()
        );
        drop(metadata);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), original);
    }
}

#[tokio::test]
async fn plain_tables_reject_bad_schemas_keys_and_consumed_null_cells() {
    for sql in [
        "DROP TABLE page",
        "ALTER TABLE title ALTER TitleID TYPE BIGINT",
        "INSERT INTO title SELECT * FROM title",
        "UPDATE title SET TitleID = 'invalid'",
    ] {
        let (_directory, path, _) = plain_database().await;
        update_database(&path, sql);
        assert!(
            BhlMetadata::open_tables(&path, "snapshot").is_err(),
            "{sql}"
        );
    }
    let (_directory, path, _) = plain_database().await;
    update_database(&path, "UPDATE title SET FullTitle = NULL");
    let metadata = BhlMetadata::open_tables(&path, "snapshot").unwrap();
    assert!(
        metadata
            .for_each(&[ObjectRef::new(ITEM)], |_| Ok(()))
            .is_err()
    );
    assert!(BhlMetadata::open_tables(&path, "  ").is_err());
    let missing = path.with_file_name("missing.duckdb");
    assert!(BhlMetadata::open_tables(&missing, "snapshot").is_err());
    assert!(!tokio::fs::try_exists(&missing).await.unwrap());
}

#[tokio::test]
async fn part_parent_associations_preserve_blank_ids_and_do_not_multiply_pages() {
    let (_root, storage) = source_table("part", "part-multi-item.txt.gz").await;
    let metadata = BhlMetadata::load(storage).await.unwrap();
    assert_eq!(metadata.info.sources["part"].rows, 2);
    let rows = read(&metadata, &[ITEM, PART]);
    assert_eq!(rows.len(), 2);
    assert!(list(&rows[0].metadata, "part", "PartID").is_empty());
    for row in &rows[1..] {
        assert_eq!(list(&row.metadata, "part", "PartID"), ["8", "8"]);
        assert_eq!(list(&row.metadata, "part", "ItemID"), ["2", ""]);
        assert_eq!(cell(&row.metadata, "item", "ItemID"), "2");
        assert!(row.missing.is_empty());
        assert!(row.conflicts.is_empty());
    }
    execute(&metadata, "UPDATE part SET ItemID = '9' WHERE ItemID = '';");
    let row = read(&metadata, &[PART]).pop().unwrap();
    assert_eq!(row.conflicts, ["part.ItemID=9, selected item=Some(2)"]);
    execute(&metadata, "DELETE FROM page; DELETE FROM partpage;");
    invalid_selection(&metadata, &[PART]);
}

#[tokio::test]
async fn page_relations_preserve_types_order_and_one_output_per_ocr_request() {
    let (root, storage) = source("title.txt.gz").await;
    tokio::fs::write(
        root.path().join("data/page.txt.gz"),
        fixture!("bhl-metadata", "page-multiple-types.txt.gz").await,
    )
    .await
    .unwrap();
    let metadata = BhlMetadata::load(storage).await.unwrap();
    assert_eq!(metadata.info.sources["page"].rows, 3);
    let rows = read(&metadata, &[ITEM, PART]);
    assert_eq!(rows.len(), 2);
    for row in &rows[..2] {
        assert!(row.missing.is_empty());
        assert!(row.conflicts.is_empty());
        assert_eq!(
            list(&row.metadata, "page", "PageTypeName"),
            ["Text", "Illustration", "Text"]
        );
        assert_eq!(list(&row.metadata, "page", "PageNumber"), ["I", "1", "I"]);
        assert_eq!(list(&row.metadata, "page", "ItemID"), ["2", "2", "2"]);
    }
    execute(
        &metadata,
        "INSERT INTO page VALUES ('3','7','6','','','','','','Text','');",
    );
    invalid_selection(&metadata, &[ITEM]);
}

#[tokio::test]
async fn managed_manifests_still_require_source_checksums() {
    let (_root, storage) = source("title.txt.gz").await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("managed.duckdb");
    BhlMetadata::create(storage, &path).await.unwrap();
    let db = ::duckdb::Connection::open(&path).unwrap();
    let info: String = db
        .query_row("SELECT info FROM __bhl_metadata", [], |row| row.get(0))
        .unwrap();
    let mut info: serde_json::Value = serde_json::from_str(&info).unwrap();
    info["sources"]["page"]
        .as_object_mut()
        .unwrap()
        .remove("sha256");
    db.execute("UPDATE __bhl_metadata SET info = ?", [info.to_string()])
        .unwrap();
    drop(db);
    assert!(BhlMetadata::open(&path).is_err());
}
