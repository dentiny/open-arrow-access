use super::*;

fn spill(connection: &ManagedConnection) {
    connection
        .execute_batch(
            "SET memory_limit = '16MB';
             SET threads = 1;
             SET preserve_insertion_order = false;
             CREATE TEMP TABLE spill_test AS
             SELECT i, repeat(md5(i::VARCHAR), 8) AS payload FROM range(200000) t(i);",
        )
        .unwrap();
    let bytes: u64 = connection
        .query_row("SELECT sum(length(payload)) FROM spill_test", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(bytes, 200000 * 256);
    let files: u64 = connection
        .query_row("SELECT count(*) FROM duckdb_temporary_files()", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(files > 0, "test must exercise disk spilling");
}

#[test]
fn clones_keep_scratch_alive_until_the_last_connection_closes() {
    let connection = ManagedConnection::in_memory().unwrap();
    let directory = connection.directory.path().to_path_buf();
    let clone = connection.try_clone().unwrap();
    let other = ManagedConnection::in_memory().unwrap();
    assert_ne!(directory, other.directory.path());
    drop(connection);
    assert!(directory.exists());
    spill(&clone);
    clone.close().unwrap();
    assert!(!directory.exists());
}

#[test]
fn persistent_connections_spill_to_local_scratch() {
    let database = tempfile::tempdir().unwrap();
    let path = database.path().join("metadata.duckdb");
    let writer = ManagedConnection::open(&path, false).unwrap();
    spill(&writer);
    writer.close().unwrap();
    let reader = ManagedConnection::open(&path, true).unwrap();
    spill(&reader);
    reader.close().unwrap();
    assert!(!path.with_extension("duckdb.tmp").exists());
}

#[cfg(unix)]
#[test]
fn spills_without_a_writable_working_directory() {
    const CHILD: &str = "DUCKDB_SCRATCH_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let connection = ManagedConnection::in_memory().unwrap();
        let path: String = connection
            .query_row("SELECT current_setting('temp_directory')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(Path::new(&path).is_absolute());
        spill(&connection);
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    // A child process avoids changing the working directory of parallel tests.
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "duckdb::connection::tests::spills_without_a_writable_working_directory",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .current_dir(directory.path())
        .status()
        .unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(status.success());
    assert!(!directory.path().join(".tmp").exists());
}

#[test]
fn temporary_database_and_spill_files_outlive_the_original_connection() {
    let connection = ManagedConnection::temporary().unwrap();
    let directory = connection.directory.path().to_path_buf();
    assert!(directory.join("snapshot.duckdb").exists());
    let clone = connection.try_clone().unwrap();
    drop(connection);
    spill(&clone);
    clone.close().unwrap();
    assert!(!directory.exists());
}
