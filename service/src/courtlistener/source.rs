//! Stage one: import the four typed input streams concurrently, as in BHL metadata.
use super::{
    CourtListenerCopyDialect, CourtListenerSchemas, CourtListenerSnapshot,
    CourtListenerTableAccessor,
    error::{arrow_error, db_error, scratch_error, task_error},
    valdiation::validate_keys,
};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use duckdb::{Config, Connection};
use futures::future::join_all;
use open_arrow_access_core::{Accessor, ByteAccessor, ReadRequest, Result};
use std::sync::Arc;

pub(super) const TABLES: [&str; 4] = ["opinions", "opinion-clusters", "dockets", "courts"];
pub(super) const ORDINAL: &str = "__courtlistener_ordinal";

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| task_error("courtlistener_duckdb_task", e))?
}
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn sql_type(dtype: &DataType) -> &'static str {
    match dtype {
        DataType::Utf8 => "VARCHAR",
        DataType::Boolean => "BOOLEAN",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt64 => "UBIGINT",
        DataType::Float64 => "DOUBLE",
        DataType::Date32 => "DATE",
        DataType::Timestamp(TimeUnit::Microsecond, _) => "TIMESTAMPTZ",
        _ => unreachable!("validated schemas"),
    }
}
pub(super) struct Database {
    pub connection: Connection,
    _directory: tempfile::TempDir,
}
impl Database {
    fn new(schemas: &CourtListenerSchemas) -> Result<Self> {
        // Store imported tables and DuckDB join/sort spill files on temporary disk.
        // Database owns the directory through query completion; dropping it removes the files.
        let directory = tempfile::tempdir().map_err(scratch_error)?;
        let config = Config::default()
            .with(
                "temp_directory",
                directory.path().join("spill").to_string_lossy(),
            )
            .map_err(db_error)?;
        let connection =
            Connection::open_with_flags(directory.path().join("snapshot.duckdb"), config)
                .map_err(db_error)?;
        for (i, fields) in schemas.tables().iter().enumerate() {
            let columns = fields
                .iter()
                .map(|f| {
                    format!(
                        "{} {}{}",
                        quoted(f.name()),
                        sql_type(f.data_type()),
                        if f.is_nullable() { "" } else { " NOT NULL" }
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            connection
                .execute_batch(&format!(
                    "CREATE TABLE cl_{i} ({columns}, {} UBIGINT NOT NULL)",
                    quoted(ORDINAL)
                ))
                .map_err(db_error)?;
        }
        Ok(Self {
            connection,
            _directory: directory,
        })
    }
}
fn append(connection: &Connection, table: usize, batch: RecordBatch, ordinal: u64) -> Result<()> {
    let count = batch.num_rows();
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(ORDINAL, DataType::UInt64, false)));
    let mut arrays = batch.columns().to_vec();
    arrays.push(Arc::new(UInt64Array::from_iter_values(
        ordinal..ordinal + count as u64,
    )));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(arrow_error)?;
    let mut appender = connection
        .appender(&format!("cl_{table}"))
        .map_err(db_error)?;
    appender.append_record_batch(batch).map_err(db_error)?;
    appender.flush().map_err(db_error)
}
async fn import_table<A: ByteAccessor + 'static>(
    mut connection: Connection,
    source: CourtListenerTableAccessor<Arc<A>>,
    request: ReadRequest,
    table: usize,
) -> Result<()> {
    let mut reader = source.open(request).await?;
    let mut ordinal = 0;
    while let Some(batch) = reader.next().await? {
        let count = batch.num_rows() as u64;
        connection = blocking(move || {
            append(&connection, table, batch, ordinal)?;
            Ok(connection)
        })
        .await?;
        ordinal += count;
    }
    Ok(())
}

pub(super) async fn ingest<A: ByteAccessor + 'static>(
    storage: Arc<A>,
    dialect: CourtListenerCopyDialect,
    schemas: CourtListenerSchemas,
    request: &ReadRequest,
) -> Result<Database> {
    let source = CourtListenerTableAccessor::new(storage)
        .with_schemas(schemas.clone())?
        .with_dialect(dialect)?;
    let database = blocking(move || Database::new(&schemas)).await?;
    let mut imports = Vec::new();
    for (i, table) in TABLES.iter().enumerate() {
        let connection = database.connection.try_clone().map_err(db_error)?;
        let object = CourtListenerSnapshot::new(*table, request.object.locator())?.object();
        imports.push(import_table(
            connection,
            source.clone(),
            ReadRequest::new(object).with_byte_options(request.byte_options),
            i,
        ));
    }
    // Finish every importer before returning, including when one fails (BHL pattern).
    for imported in join_all(imports).await {
        imported?;
    }
    blocking(move || {
        validate_keys(&database.connection, &TABLES)?;
        Ok(database)
    })
    .await
}
