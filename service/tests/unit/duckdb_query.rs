use super::*;
use arrow_array::{Array, Int64Array, StringArray, StructArray};
use arrow_schema::DataType;
use open_arrow_access_core::{Error, ErrorKind};

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

#[test]
fn queries_existing_metadata_with_parameters_and_arrow_types() {
    let (_directory, path) = database();
    let db = DuckDbDatabase::open(&path).unwrap();
    let mut output = Vec::new();
    let schema = db
        .query(
            "SELECT id, title, provenance FROM metadata WHERE id >= ? ORDER BY id",
            crate::duckdb::params![9998_i64],
            |batch| {
                output.push(batch);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let ids = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 9998);
    let titles = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(titles.value(0), "title-9998");
    let provenance = output[0]
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(provenance.len(), 2);
}

#[test]
fn streams_multiple_batches_and_returns_empty_result_schema() {
    let (_directory, path) = database();
    let db = DuckDbDatabase::open(&path).unwrap();
    let (mut rows, mut batches) = (0, 0);
    db.query("SELECT * FROM metadata", [], |batch| {
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
fn missing_and_invalid_files_are_errors_without_creating_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing.duckdb");
    assert!(DuckDbDatabase::open(&path).is_err());
    assert!(!path.exists());
    std::fs::write(&path, "not a database").unwrap();
    assert!(DuckDbDatabase::open(&path).is_err());
}

#[test]
fn rejects_writes_and_invalid_queries_and_remains_usable() {
    let (_directory, path) = database();
    let original = std::fs::read(&path).unwrap();
    let db = DuckDbDatabase::open(&path).unwrap();
    for sql in [
        "DELETE FROM metadata",
        "CREATE TABLE unwanted(id INTEGER)",
        "SELECT * FROM missing_table",
        "SELEC invalid",
        "SELECT CAST(title AS BIGINT) FROM metadata",
    ] {
        assert!(db.query(sql, [], |_| Ok(())).is_err(), "{sql}");
    }
    assert!(db.query("SELECT ?", [], |_| Ok(())).is_err());
    db.query("SELECT count(*) FROM metadata", [], |_| Ok(()))
        .unwrap();
    drop(db);
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

#[test]
fn callback_failure_stops_emission_and_preserves_error() {
    let (_directory, path) = database();
    let db = DuckDbDatabase::open(&path).unwrap();
    let mut calls = 0;
    let error = db
        .query("SELECT * FROM metadata", [], |_| {
            calls += 1;
            Err(Error::new(ErrorKind::InvalidInput, "consumer", "stop"))
        })
        .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), "consumer");
    db.query("SELECT 1", [], |_| Ok(())).unwrap();
}

fn assert_row_count(db: &DuckDbDatabase) {
    db.query("SELECT count(*) FROM metadata", [], |batch| {
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 10000);
        Ok(())
    })
    .unwrap();
}

#[tokio::test]
async fn stages_chunked_storage_and_keeps_file_until_database_drops() {
    use open_arrow_access_core::ByteReadOptions;
    use open_arrow_access_test_support::MemoryAccessor;
    use std::num::NonZeroUsize;

    let (directory, path) = database();
    let storage = MemoryAccessor::new(&std::fs::read(path).unwrap(), 4096).without_content_length();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: NonZeroUsize::new(1024),
    };
    let db = DuckDbDatabase::open_from(
        &storage,
        ByteReadRequest::new("s3://bucket/metadata.duckdb").with_options(options),
    )
    .await
    .unwrap();
    let staged = db
        .connection
        .source_directory
        .as_ref()
        .unwrap()
        .path()
        .to_path_buf();
    assert!(staged.join("source.duckdb").exists());
    let requests = storage.observations.requests.lock().unwrap();
    assert_eq!(requests[0].object.locator(), "s3://bucket/metadata.duckdb");
    assert_eq!(requests[0].options, options);
    drop(requests);
    drop(storage);
    drop(directory);
    let clone = db.try_clone().unwrap();
    drop(db);
    assert_row_count(&clone);
    assert!(staged.exists());
    drop(clone);
    assert!(!staged.exists());
}

#[tokio::test]
async fn rejects_failed_truncated_and_invalid_remote_downloads() {
    use open_arrow_access_test_support::{MemoryAccessor, expect_err_kind};
    let (_directory, path) = database();
    let bytes = std::fs::read(path).unwrap();
    let storage = MemoryAccessor::new(&bytes, 4096).with_failure(4096, ErrorKind::PermissionDenied);
    expect_err_kind(
        DuckDbDatabase::open_from(&storage, "key").await,
        ErrorKind::PermissionDenied,
    );
    let storage = MemoryAccessor::new(&bytes, 4096).with_open_failure(ErrorKind::NotFound);
    expect_err_kind(
        DuckDbDatabase::open_from(&storage, "key").await,
        ErrorKind::NotFound,
    );
    let storage = MemoryAccessor::new(&bytes, 4096).without_content_length();
    for length in [bytes.len() as u64 - 1, bytes.len() as u64 + 1] {
        let error = expect_err_kind(
            DuckDbDatabase::open_from(
                &storage,
                ByteReadRequest::new("key").with_content_length(length),
            )
            .await,
            ErrorKind::Io,
        );
        assert_eq!(error.operation(), "duckdb_download");
    }
    let storage = MemoryAccessor::new(b"not a database", 4);
    expect_err_kind(
        DuckDbDatabase::open_from(&storage, "key").await,
        ErrorKind::Io,
    );
}

#[tokio::test]
async fn queries_http_database_using_configured_storage() {
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::http::HttpFixture;
    let (_directory, path) = database();
    let fixture = HttpFixture::new();
    fixture.file("/data/metadata.duckdb", std::fs::read(path).unwrap());
    let operator = opendal::Operator::new(
        opendal_service_http::Http::default()
            .endpoint(fixture.endpoint())
            .root("/data"),
    )
    .unwrap()
    .with_context(fixture.context());
    let storage = OpenDalAccessor::new(operator);
    let db = DuckDbDatabase::open_from(&storage, "metadata.duckdb")
        .await
        .unwrap();
    assert_row_count(&db);
    assert!(
        fixture
            .requests()
            .iter()
            .any(|request| request.method == http::Method::GET)
    );
}

#[tokio::test]
async fn queries_s3_database_and_uses_caller_credentials() {
    use open_arrow_access_storage::OpenDalAccessor;
    use open_arrow_access_test_support::http::HttpFixture;
    let (_directory, path) = database();
    let fixture = HttpFixture::new();
    fixture.file("/bucket/data/metadata.duckdb", std::fs::read(path).unwrap());
    let operator = opendal::Operator::new(
        opendal::services::S3::default()
            .endpoint(fixture.endpoint())
            .bucket("bucket")
            .region("us-east-1")
            .root("/data")
            .access_key_id("fixture-access-key")
            .secret_access_key("fixture-secret-key")
            .disable_config_load(),
    )
    .unwrap()
    .with_context(fixture.context());
    let storage = OpenDalAccessor::new(operator);
    let db = DuckDbDatabase::open_from(&storage, "metadata.duckdb")
        .await
        .unwrap();
    assert_row_count(&db);
    let requests = fixture.requests();
    assert!(requests.iter().any(|request| {
        request
            .headers
            .get("authorization")
            .is_some_and(|value| value.to_str().unwrap().contains("fixture-access-key"))
    }));
}

#[test]
fn initializes_sql_before_attach_and_keeps_database_read_only() {
    let (directory, path) = database();
    let script = directory.path().join("secrets.sql");
    std::fs::write(&script, "SELECT CASE WHEN current_database() = 'memory' THEN 1 ELSE error('setup ran after attach') END; CREATE MACRO initialized() AS 42;").unwrap();
    let db = DuckDbDatabase::connect(path.to_str().unwrap(), Some(&script)).unwrap();
    db.query("SELECT memory.initialized()", [], |batch| {
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    })
    .unwrap();
    assert!(db.query("DELETE FROM metadata", [], |_| Ok(())).is_err());
    let clone = db.try_clone().unwrap();
    drop(db);
    assert_row_count(&clone);
}

#[test]
fn setup_errors_do_not_expose_script_credentials_or_create_missing_files() {
    let (directory, path) = database();
    let script = directory.path().join("secrets.sql");
    std::fs::write(&script, "invalid sql 'private-test-credential'").unwrap();
    let error = DuckDbDatabase::connect(path.to_str().unwrap(), Some(&script))
        .err()
        .unwrap();
    assert_eq!(error.operation(), "duckdb_secret_setup");
    assert!(!format!("{error:?}").contains("private-test-credential"));
    assert!(
        DuckDbDatabase::connect(
            path.to_str().unwrap(),
            Some(&directory.path().join("missing.sql"))
        )
        .is_err()
    );
    let missing = directory.path().join("missing.duckdb");
    assert!(DuckDbDatabase::connect(missing.to_str().unwrap(), None).is_err());
    assert!(!missing.exists());
}

#[test]
#[ignore = "requires the matching DuckDB httpfs extension installed locally"]
fn creates_real_secrets_visible_to_cloned_connections() {
    let (directory, path) = database();
    let script = directory.path().join("secrets.sql");
    std::fs::write(&script, "LOAD httpfs; CREATE SECRET fixture_credentials (TYPE s3, KEY_ID 'fixture-key', SECRET 'fixture-value', REGION 'us-east-1');").unwrap();
    let db = DuckDbDatabase::connect(path.to_str().unwrap(), Some(&script)).unwrap();
    let clone = db.try_clone().unwrap();
    drop(db);
    let count: i64 = clone.connection.query_row("SELECT count(*) FROM duckdb_secrets() WHERE name = 'fixture_credentials' AND NOT persistent", [], |row| row.get(0)).unwrap();
    assert_eq!(count, 1);
    assert_row_count(&clone);
}

#[test]
#[ignore = "requires an authenticated range-capable HTTP fixture and installed httpfs"]
fn opens_authenticated_remote_database_after_creating_secret() {
    let (directory, path) = database();
    let served = std::env::var("DUCKDB_TEST_HTTP_DIRECTORY").unwrap();
    std::fs::copy(path, std::path::Path::new(&served).join("source.duckdb")).unwrap();
    let url = std::env::var("DUCKDB_TEST_HTTP_URL").unwrap();
    let script = directory.path().join("secrets.sql");
    std::fs::write(
        &script,
        "LOAD httpfs; CREATE SECRET fixture_http (TYPE http, BEARER_TOKEN 'fixture-token');",
    )
    .unwrap();
    let db = DuckDbDatabase::connect(&url, Some(&script)).unwrap();
    db.query(
        "SELECT CAST(sum(id) AS BIGINT) FROM metadata",
        [],
        |batch| {
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                49_995_000
            );
            Ok(())
        },
    )
    .unwrap();
    assert_row_count(&db);
    let clone = db.try_clone().unwrap();
    drop(db);
    assert_row_count(&clone);
    assert!(clone.query("DELETE FROM metadata", [], |_| Ok(())).is_err());
}

#[test]
fn clones_preserve_selected_schema_and_full_search_path() {
    let (_directory, path) = database();
    let connection = ::duckdb::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE SCHEMA custom; CREATE TABLE custom.only_custom AS SELECT 7 AS value;",
        )
        .unwrap();
    drop(connection);
    let db = DuckDbDatabase::connect(path.to_str().unwrap(), None).unwrap();
    db.query(
        "SET search_path = 'source_database.custom,source_database.main'",
        [],
        |_| Ok(()),
    )
    .unwrap();
    let clone = db.try_clone().unwrap();
    drop(db);
    clone
        .query("SELECT * FROM only_custom", [], |batch| {
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .value(0),
                7
            );
            Ok(())
        })
        .unwrap();
    assert_row_count(&clone);
}

#[test]
#[ignore = "requires a local S3 HTTP fixture and installed httpfs"]
fn opens_s3_database_with_secret_sql_and_reads_actual_rows() {
    let (directory, path) = database();
    let served = std::env::var("DUCKDB_TEST_HTTP_DIRECTORY").unwrap();
    std::fs::copy(path, std::path::Path::new(&served).join("source.duckdb")).unwrap();
    let endpoint = std::env::var("DUCKDB_TEST_S3_ENDPOINT").unwrap();
    let script = directory.path().join("s3-secrets.sql");
    std::fs::write(&script, format!("LOAD httpfs; CREATE SECRET fixture_s3 (TYPE s3, KEY_ID 'fixture-key', SECRET 'fixture-value', REGION 'us-east-1', ENDPOINT '{}', USE_SSL false, URL_STYLE 'path');", endpoint.replace('\'', "''"))).unwrap();
    let db = DuckDbDatabase::connect("s3://fixture-bucket/source.duckdb", Some(&script)).unwrap();
    let clone = db.try_clone().unwrap();
    drop(db);
    let mut rows = 0;
    clone
        .query(
            "SELECT id, title FROM metadata WHERE id >= 9998 ORDER BY id",
            [],
            |batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let titles = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for index in 0..batch.num_rows() {
                    assert_eq!(ids.value(index), 9998 + rows);
                    assert_eq!(titles.value(index), format!("title-{}", 9998 + rows));
                    rows += 1;
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(rows, 2);
}
