//! Stage one: import the four typed input streams concurrently, as in BHL metadata.
use super::{
    CourtListenerCopyDialect, CourtListenerSchemas, CourtListenerSnapshot,
    CourtListenerTableAccessor,
    error::{arrow_error, db_error, task_error},
    validation::validate_keys,
};
use crate::duckdb::ManagedConnection;
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::future::join_all;
use open_arrow_access_core::{Accessor, ByteAccessor, ReadRequest, Result};
use std::sync::Arc;
use tokio::sync::mpsc;

pub(super) const TABLES: [&str; 4] = ["opinions", "opinion-clusters", "dockets", "courts"];
pub(super) const DB_TABLES: [&str; 4] = ["opinions", "opinion_clusters", "dockets", "courts"];
pub(super) const ORDINAL: &str = "__courtlistener_ordinal";

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
pub(super) fn sql_type(dtype: &DataType) -> &'static str {
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
    pub connection: ManagedConnection,
}
impl Database {
    fn new(schemas: &CourtListenerSchemas, connection: Option<ManagedConnection>) -> Result<Self> {
        let connection = match connection {
            Some(connection) => connection,
            None => ManagedConnection::temporary()?,
        };
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
                    "CREATE TABLE {} ({columns}, {} UBIGINT NOT NULL)",
                    DB_TABLES[i],
                    quoted(ORDINAL)
                ))
                .map_err(db_error)?;
        }
        Ok(Self { connection })
    }
}
async fn import_table<A: ByteAccessor + 'static>(
    connection: ManagedConnection,
    source: CourtListenerTableAccessor<Arc<A>>,
    request: ReadRequest,
    table: usize,
) -> Result<()> {
    let mut reader = source.open(request).await?;
    // Bound read-ahead while one blocking importer owns the table's appender.
    let (sender, mut receiver) = mpsc::channel::<RecordBatch>(1);
    let task = tokio::task::spawn_blocking(move || -> Result<_> {
        connection
            .execute_batch("BEGIN TRANSACTION")
            .map_err(db_error)?;
        let mut appender = connection.appender(DB_TABLES[table]).map_err(db_error)?;
        let mut ordinal = 0u64;
        while let Some(batch) = receiver.blocking_recv() {
            let end = ordinal + batch.num_rows() as u64;
            let mut fields = batch.schema().fields().to_vec();
            fields.push(Arc::new(Field::new(ORDINAL, DataType::UInt64, false)));
            let mut arrays = batch.columns().to_vec();
            arrays.push(Arc::new(UInt64Array::from_iter_values(ordinal..end)));
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(arrow_error)?;
            appender.append_record_batch(batch).map_err(db_error)?;
            ordinal = end;
        }
        // Appender::drop suppresses write errors; flush explicitly once per table.
        appender.flush().map_err(db_error)?;
        drop(appender);
        Ok(connection)
    });
    let read_result = async {
        while let Some(batch) = reader.next().await? {
            if sender.send(batch).await.is_err() {
                break;
            }
        }
        Ok::<_, open_arrow_access_core::Error>(())
    }
    .await;
    drop(sender);
    // Join the importer even on read failure, and commit only after both succeed.
    let imported = task
        .await
        .map_err(|e| task_error("courtlistener_duckdb_task", e))?;
    read_result?;
    let connection = imported?;
    tokio::task::spawn_blocking(move || connection.execute_batch("COMMIT").map_err(db_error))
        .await
        .map_err(|e| task_error("courtlistener_duckdb_task", e))?
}

pub(super) async fn ingest<A: ByteAccessor + 'static>(
    storage: Arc<A>,
    dialect: CourtListenerCopyDialect,
    schemas: CourtListenerSchemas,
    request: &ReadRequest,
    connection: Option<ManagedConnection>,
) -> Result<Database> {
    let source = CourtListenerTableAccessor::new(storage)
        .with_schemas(schemas.clone())?
        .with_dialect(dialect)?;
    let database = tokio::task::spawn_blocking(move || Database::new(&schemas, connection))
        .await
        .map_err(|e| task_error("courtlistener_duckdb_task", e))??;
    let mut imports = Vec::new();
    for (i, table) in TABLES.iter().enumerate() {
        let connection = database.connection.try_clone()?;
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
    tokio::task::spawn_blocking(move || -> Result<_> {
        validate_keys(&database.connection, &DB_TABLES)?;
        Ok(database)
    })
    .await
    .map_err(|e| task_error("courtlistener_duckdb_task", e))?
}
