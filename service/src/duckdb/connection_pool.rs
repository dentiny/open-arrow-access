use super::DuckDbDatabase;
use open_arrow_access_core::{Error, ErrorKind, Result};
use std::{ops::Deref, sync::Mutex};

/// Reuse connections and their prepared statements while sharing one database.
/// The pool grows on demand with concurrent checkouts and retains idle connections.
pub struct DuckDbConnectionPool {
    connections: Mutex<Connections>,
}

struct Connections {
    database: DuckDbDatabase,
    idle: Vec<DuckDbDatabase>,
}

impl DuckDbConnectionPool {
    pub fn new(database: DuckDbDatabase) -> Self {
        Self {
            connections: Mutex::new(Connections {
                database,
                idle: Vec::new(),
            }),
        }
    }

    /// Check out an idle connection, cloning one only when all are in use.
    /// The returned guard returns it to the pool on drop, including on errors.
    /// No pool lock is held while callers execute queries.
    pub fn get_connection(&self) -> Result<PooledConnection<'_>> {
        let mut connections = self.connections.lock().map_err(|_| {
            Error::new(
                ErrorKind::Io,
                "duckdb_pool",
                "connection pool lock poisoned",
            )
        })?;
        let connection = match connections.idle.pop() {
            Some(connection) => connection,
            None => connections.database.try_clone()?,
        };
        Ok(PooledConnection {
            pool: self,
            connection: Some(connection),
        })
    }
}

/// A checked-out connection, automatically returned to its pool on drop.
pub struct PooledConnection<'a> {
    pool: &'a DuckDbConnectionPool,
    connection: Option<DuckDbDatabase>,
}

impl Deref for PooledConnection<'_> {
    type Target = DuckDbDatabase;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("pooled connection is present until drop")
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take()
            && let Ok(mut connections) = self.pool.connections.lock()
        {
            connections.idle.push(connection);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_connection_on_error_and_preserves_connection_local_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pool.duckdb");
        ::duckdb::Connection::open(&path).unwrap().close().unwrap();
        let pool = DuckDbConnectionPool::new(DuckDbDatabase::open(path).unwrap());
        let result = (|| -> Result<()> {
            let connection = pool.get_connection()?;
            connection.query(
                "CREATE TEMP TABLE checkout_state AS SELECT 42 AS value",
                [],
                |_| Ok(()),
            )?;
            // Another simultaneous checkout must use a distinct connection.
            let other = pool.get_connection()?;
            assert!(
                other
                    .query("SELECT * FROM checkout_state", [], |_| Ok(()))
                    .is_err()
            );
            connection.query("SELECT * FROM missing_table", [], |_| Ok(()))?;
            Ok(())
        })();
        assert!(result.is_err());
        // Error propagation drops the guard. Reusing that connection retains
        // its temporary table, which a newly cloned connection would not see.
        let connection = pool.get_connection().unwrap();
        let mut rows = 0;
        connection
            .query("SELECT * FROM checkout_state", [], |batch| {
                rows += batch.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 1);
    }
}
