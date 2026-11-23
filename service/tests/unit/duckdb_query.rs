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
fn cached_query_rebinds_and_resets_after_an_interrupted_result() {
    let (_directory, path) = database();
    let db = DuckDbDatabase::open(path).unwrap();
    let sql = "SELECT id FROM metadata WHERE id >= ? ORDER BY id";
    // Leave an unread multi-batch result in the cached statement.
    let result = db.query_cached(sql, crate::duckdb::params![0_i64], |_| {
        Err(error::scratch(std::io::Error::other(
            "stop after one batch",
        )))
    });
    assert!(result.is_err());
    for (start, expected) in [
        (9998_i64, vec![9998, 9999]),
        (10000, vec![]),
        (9999, vec![9999]),
    ] {
        let mut rows = Vec::new();
        let schema = db
            .query_cached(sql, crate::duckdb::params![start], |batch| {
                rows.extend(
                    batch
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .iter()
                        .copied(),
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(rows, expected);
    }
}
