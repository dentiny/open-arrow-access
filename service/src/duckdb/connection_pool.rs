use super::DuckDbDatabase;
use open_arrow_access_core::{Error, ErrorKind, Result};
use std::{collections::VecDeque, ops::Deref, sync::Mutex};

const DEFAULT_MAX_IDLE: usize = 16;

/// Reuse connections and their prepared statements while sharing one database.
/// Active checkouts grow on demand; retained idle connections are bounded.
pub struct DuckDbConnectionPool {
    connections: Mutex<Connections>,
    max_idle: usize,
}

struct Connections {
    database: DuckDbDatabase,
    idle: VecDeque<DuckDbDatabase>,
}

impl DuckDbConnectionPool {
    /// Retain at most 16 idle connections.
    pub fn new(database: DuckDbDatabase) -> Self {
        Self::with_max_idle(database, DEFAULT_MAX_IDLE)
    }

    /// Retain at most `max_idle` connections, evicting the least recently returned.
    /// Zero disables idle retention. Active checkouts are not limited.
    pub fn with_max_idle(database: DuckDbDatabase, max_idle: usize) -> Self {
        Self {
            connections: Mutex::new(Connections {
                database,
                idle: VecDeque::new(),
            }),
            max_idle,
        }
    }

    /// Check out the most recently returned connection, or clone one if none is idle.
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
        let connection = match connections.idle.pop_back() {
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
        let Some(connection) = self.connection.take() else {
            return;
        };
        if self.pool.max_idle == 0 {
            return;
        }
        let evicted = {
            let Ok(mut connections) = self.pool.connections.lock() else {
                return;
            };
            let evicted = if connections.idle.len() == self.pool.max_idle {
                connections.idle.pop_front()
            } else {
                None
            };
            connections.idle.push_back(connection);
            evicted
        };
        // Closing a connection must not block other checkouts behind the pool lock.
        drop(evicted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> (tempfile::TempDir, DuckDbDatabase) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pool.duckdb");
        ::duckdb::Connection::open(&path).unwrap().close().unwrap();
        let database = DuckDbDatabase::open(path).unwrap();
        (directory, database)
    }

    #[test]
    fn bounds_idle_connections_and_retains_the_most_recently_returned() {
        for max_idle in [0, 1, 2] {
            let (_directory, database) = database();
            let pool = DuckDbConnectionPool::with_max_idle(database, max_idle);
            let connections: Vec<_> = (0..4).map(|_| pool.get_connection().unwrap()).collect();
            connections
                .last()
                .unwrap()
                .query(
                    "CREATE TEMP TABLE newest_connection AS SELECT 1",
                    [],
                    |_| Ok(()),
                )
                .unwrap();
            for connection in connections {
                drop(connection);
            }
            assert_eq!(pool.connections.lock().unwrap().idle.len(), max_idle);
            let connection = pool.get_connection().unwrap();
            let result = connection.query("SELECT * FROM newest_connection", [], |_| Ok(()));
            assert_eq!(result.is_ok(), max_idle > 0);
        }
    }
}
