use super::*;
use arrow_array::{cast::AsArray, types::Int64Type};
use arrow_schema::DataType;

fn database() -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata '测试.duckdb");
    let connection = ::duckdb::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE metadata AS SELECT i AS id, 'title-' || i AS title,
             struct_pack(source := 'BHL', revision := 1) AS provenance
             FROM range(10000) t(i); CHECKPOINT",
        )
        .unwrap();
    connection.close().unwrap();
    (directory, path)
}

fn assert_metadata(db: &DuckDbDatabase) {
    let mut rows = Vec::new();
    let schema = db
        .query(
            "SELECT id, title, provenance FROM metadata WHERE id >= ? ORDER BY id",
            crate::duckdb::params![9998_i64],
            |batch| {
                let ids = batch.column(0).as_primitive::<Int64Type>();
                let titles = batch.column(1).as_string::<i32>();
                let sources = batch
                    .column(2)
                    .as_struct()
                    .column_by_name("source")
                    .unwrap()
                    .as_string::<i32>();
                for row in 0..batch.num_rows() {
                    rows.push((ids.value(row), titles.value(row).to_owned()));
                    assert_eq!(sources.value(row), "BHL");
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        rows,
        [
            (9998, "title-9998".to_owned()),
            (9999, "title-9999".to_owned())
        ]
    );
}

fn remote_database(location: &str, secret_sql: &str) -> DuckDbDatabase {
    let (directory, path) = database();
    let served = std::env::var("DUCKDB_TEST_HTTP_DIRECTORY").unwrap();
    std::fs::copy(path, std::path::Path::new(&served).join("source.duckdb")).unwrap();
    let script = directory.path().join("secrets.sql");
    std::fs::write(&script, secret_sql).unwrap();
    DuckDbDatabase::connect(location, Some(&script)).unwrap()
}

#[test]
fn queries_local_metadata() {
    let (_directory, path) = database();
    assert_metadata(&DuckDbDatabase::open(path).unwrap());
}

#[test]
fn streams_batches_and_returns_empty_result_schema() {
    let (_directory, path) = database();
    let db = DuckDbDatabase::open(path).unwrap();
    let (mut rows, mut batches) = (0, 0);
    db.query("SELECT id FROM metadata", [], |batch| {
        rows += batch.num_rows();
        batches += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(rows, 10000);
    assert!(batches > 1);
    let schema = db
        .query("SELECT id FROM metadata WHERE false", [], |batch| {
            assert_eq!(batch.num_rows(), 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
}

#[test]
#[ignore = "requires community extension installation"]
fn cloned_connections_retain_secret_sql_and_search_path() {
    let (directory, path) = database();
    let script = directory.path().join("secrets.sql");
    std::fs::write(
        &script,
        "LOAD httpfs;
         CREATE SECRET fixture (TYPE s3, KEY_ID 'fixture-key', SECRET 'fixture-value', REGION 'us-east-1');
         CREATE SCHEMA custom;
         CREATE TABLE custom.initialized AS SELECT 42::BIGINT AS value;",
    )
    .unwrap();
    let db = DuckDbDatabase::connect(path.to_str().unwrap(), Some(&script)).unwrap();
    db.query(
        "SET search_path = 'memory.custom,source_database.main'",
        [],
        |_| Ok(()),
    )
    .unwrap();
    let clone = db.try_clone().unwrap();
    drop(db);
    assert_metadata(&clone);
    let value: i64 = clone
        .connection
        .query_row("SELECT value FROM initialized", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, 42);
    let secrets: i64 = clone
        .connection
        .query_row(
            "SELECT count(*) FROM duckdb_secrets() WHERE name = 'fixture' AND NOT persistent",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(secrets, 1);
}

#[test]
#[ignore = "requires an authenticated HTTP fixture and community extension installation"]
fn http_queries_share_and_clean_up_disk_cache() {
    let url = std::env::var("DUCKDB_TEST_HTTP_URL").unwrap();
    let db = remote_database(
        &url,
        "LOAD httpfs; CREATE SECRET fixture (TYPE http, BEARER_TOKEN 'fixture-token');",
    );
    let cache: String = db
        .connection
        .query_row(
            "SELECT current_setting('cache_httpfs_cache_directory')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let cache = std::path::PathBuf::from(cache);
    let cache_type: String = db
        .connection
        .query_row("SELECT current_setting('cache_httpfs_type')", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(cache_type, "on_disk");
    assert_metadata(&db);
    let clone = db.try_clone().unwrap();
    drop(db);
    assert_metadata(&clone);
    assert!(std::fs::read_dir(&cache).unwrap().next().is_some());
    drop(clone);
    assert!(!cache.exists());
}

#[test]
#[ignore = "requires a local S3 HTTP fixture and community extension installation"]
fn s3_queries_use_secret_sql() {
    let endpoint = std::env::var("DUCKDB_TEST_S3_ENDPOINT").unwrap();
    let sql = format!(
        "LOAD httpfs; CREATE SECRET fixture (TYPE s3,
        KEY_ID 'fixture-key', SECRET 'fixture-value', REGION 'us-east-1',
        ENDPOINT '{}', USE_SSL false, URL_STYLE 'path');",
        endpoint.replace('\'', "''")
    );
    let db = remote_database("s3://fixture-bucket/source.duckdb", &sql);
    assert_metadata(&db);
}
