//! Import typed Arrow batches into DuckDB, SQL-join a date, stream Arrow results.
use super::{
    CourtListenerCopyDialect, CourtListenerSchemas, CourtListenerSnapshot,
    CourtListenerTableAccessor,
    values::{date, error},
};
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use duckdb::{Config, Connection, InterruptHandle, params, types::Value};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, Error, ErrorKind, OrderRequirement, ReadRequest, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, async_trait, record_stream_reader,
};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};
use tokio::{sync::mpsc, task::JoinHandle};

const TABLES: [&str; 4] = ["opinions", "opinion-clusters", "dockets", "courts"];
const ORDINAL: &str = "__courtlistener_ordinal";

/// Caller-owned scratch, resource limits and provenance for one date conversion.
#[derive(Clone, Debug)]
pub struct CourtListenerJoinOptions {
    pub source_root: String,
    pub scratch_directory: PathBuf,
    pub max_record_bytes: NonZeroUsize,
    pub dialect: CourtListenerCopyDialect,
    /// DuckDB operator memory budget; not a whole-process RSS cap.
    pub memory_limit: NonZeroUsize,
    pub threads: NonZeroUsize,
}
impl CourtListenerJoinOptions {
    pub fn new(source_root: impl Into<String>, scratch_directory: PathBuf) -> Self {
        Self {
            source_root: source_root.into(),
            scratch_directory,
            max_record_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
            dialect: Default::default(),
            memory_limit: NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
            threads: NonZeroUsize::new(4).unwrap(),
        }
    }
}
/// A YYYY-MM-DD locator imports the four inputs and exposes one joined Arrow stream.
pub struct CourtListenerDateAccessor<A> {
    storage: Arc<A>,
    options: CourtListenerJoinOptions,
    schemas: CourtListenerSchemas,
}
impl<A> Clone for CourtListenerDateAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            options: self.options.clone(),
            schemas: self.schemas.clone(),
        }
    }
}
impl<A> CourtListenerDateAccessor<A> {
    pub fn new(storage: A, options: CourtListenerJoinOptions) -> Result<Self> {
        options.dialect.validate()?;
        if options.source_root.is_empty() {
            return Err(super::error::invalid());
        }
        Ok(Self {
            storage: Arc::new(storage),
            options,
            schemas: Default::default(),
        })
    }
    pub fn with_schemas(mut self, schemas: CourtListenerSchemas) -> Result<Self> {
        schemas.validate()?;
        if schemas
            .tables()
            .iter()
            .any(|fields| fields.iter().any(|f| f.name() == ORDINAL))
        {
            return Err(error("source field collides with reserved ordinal"));
        }
        self.schemas = schemas;
        Ok(self)
    }
    pub fn schemas(&self) -> &CourtListenerSchemas {
        &self.schemas
    }
}
fn db_error(e: duckdb::Error) -> Error {
    Error::new(ErrorKind::Io, "courtlistener_duckdb", e.to_string()).with_source(e)
}
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        Error::new(
            ErrorKind::Internal,
            "courtlistener_duckdb_task",
            e.to_string(),
        )
        .with_source(e)
    })?
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
struct Database {
    connection: Connection,
    _directory: tempfile::TempDir,
}
impl Database {
    fn new(options: CourtListenerJoinOptions, schemas: &CourtListenerSchemas) -> Result<Self> {
        let directory = tempfile::tempdir_in(options.scratch_directory).map_err(|e| {
            Error::new(ErrorKind::Io, "courtlistener_scratch", e.to_string()).with_source(e)
        })?;
        let config = Config::default()
            .with("memory_limit", format!("{}B", options.memory_limit))
            .map_err(db_error)?
            .with("threads", options.threads.to_string())
            .map_err(db_error)?
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
    fn append(&self, table: usize, batch: RecordBatch, ordinal: u64) -> Result<()> {
        let count = batch.num_rows();
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(ORDINAL, DataType::UInt64, false)));
        let mut arrays = batch.columns().to_vec();
        arrays.push(Arc::new(UInt64Array::from_iter_values(
            ordinal..ordinal + count as u64,
        )));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| error(e.to_string()))?;
        let mut appender = self
            .connection
            .appender(&format!("cl_{table}"))
            .map_err(db_error)?;
        appender.append_record_batch(batch).map_err(db_error)?;
        appender.flush().map_err(db_error)
    }
    fn validate_keys(&self) -> Result<()> {
        for (i, table) in TABLES.iter().enumerate() {
            let duplicate = self.connection.query_row(
                &format!("SELECT 1 FROM cl_{i} GROUP BY id HAVING count(*)>1 LIMIT 1"),
                [],
                |row| row.get::<_, i32>(0),
            );
            match duplicate {
                Ok(_) => return Err(error(format!("duplicate primary key in {table}"))),
                Err(duckdb::Error::QueryReturnedNoRows) => {}
                Err(e) => return Err(db_error(e)),
            }
        }
        Ok(())
    }
}

fn query_sql(schemas: &CourtListenerSchemas) -> String {
    let entity = |i: usize, alias: &str| {
        let fields = schemas.tables()[i]
            .iter()
            .map(|f| format!("{} := {alias}.{}", quoted(f.name()), quoted(f.name())))
            .collect::<Vec<_>>()
            .join(",");
        let value = format!("struct_pack({fields})");
        if i == 0 {
            value
        } else {
            format!("CASE WHEN {alias}.id IS NULL THEN NULL ELSE {value} END")
        }
    };
    format!("SELECT CAST($1 AS DATE) AS snapshot_date, {} AS opinion, {} AS cluster, {} AS docket, {} AS court,
        struct_pack(opinions_source_uri := CAST($2 AS VARCHAR), clusters_source_uri := CAST($3 AS VARCHAR), dockets_source_uri := CAST($4 AS VARCHAR), courts_source_uri := CAST($5 AS VARCHAR),
        opinion_record_index := o.{ORDINAL},cluster_record_index := c.{ORDINAL},docket_record_index := d.{ORDINAL},court_record_index := k.{ORDINAL}) AS provenance
        FROM cl_0 o LEFT JOIN cl_1 c ON o.cluster_id=c.id LEFT JOIN cl_2 d ON c.docket_id=d.id LEFT JOIN cl_3 k ON d.court_id=k.id ORDER BY o.{ORDINAL}",entity(0,"o"),entity(1,"c"),entity(2,"d"),entity(3,"k"))
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for CourtListenerDateAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        CourtListenerSnapshot::new("opinions", request.object.locator())?;
        self.schemas.validate()?;
        let schema = self.schemas.schema();
        let projection = request.options.resolve_projection(&schema)?;
        let day = date(request.object.locator())?;
        let options = self.options.clone();
        let schemas = self.schemas.clone();
        let mut database = blocking(move || Database::new(options, &schemas)).await?;
        let source = CourtListenerTableAccessor::new(self.storage.clone())
            .with_schemas(self.schemas.clone())?
            .with_dialect(self.options.dialect)?
            .with_max_record_bytes(self.options.max_record_bytes);
        let mut uris = Vec::new();
        for (i, table) in TABLES.iter().enumerate() {
            let object = CourtListenerSnapshot::new(*table, request.object.locator())?.object();
            uris.push(format!(
                "{}/{}",
                self.options.source_root.trim_end_matches('/'),
                object.locator()
            ));
            let mut reader = source
                .open(ReadRequest::new(object).with_byte_options(request.byte_options))
                .await?;
            let mut ordinal = 0;
            while let Some(batch) = reader.next().await? {
                let count = batch.num_rows() as u64;
                database = blocking(move || {
                    database.append(i, batch, ordinal)?;
                    Ok(database)
                })
                .await?;
                ordinal += count;
            }
        }
        database = blocking(move || {
            database.validate_keys()?;
            Ok(database)
        })
        .await?;
        let interrupt = database.connection.interrupt_handle();
        let state = QueryState {
            database: Some(database),
            receiver: None,
            task: None,
            interrupt,
            schema,
            sql: query_sql(&self.schemas),
            day,
            uris,
            batch_size: request.options.batch_size.get(),
        };
        let stream = stream::try_unfold(state, |mut state| async move {
            let Some(batch) = state.next().await? else {
                return Ok(None);
            };
            Ok(Some((batch, state)))
        });
        let metadata = RecordMetadata {
            schema: projection.schema,
            source: Some(request.object),
        };
        Ok(Box::new(record_stream_reader(
            stream,
            metadata,
            projection.indices,
            OrderRequirement::PreserveGlobal,
        )))
    }
}
#[async_trait]
impl<A: ByteAccessor + 'static> ScanPlanner for CourtListenerDateAccessor<A> {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        CourtListenerSnapshot::new("opinions", request.object.locator())?;
        let metadata = RecordMetadata {
            schema: request
                .options
                .resolve_projection(&self.schemas.schema())?
                .schema,
            source: Some(request.object.clone()),
        };
        let source = self.clone();
        Ok(ScanPlan::new(
            metadata,
            vec![Box::pin(async move { source.open(request).await })],
        )
        .with_ordering(OrderRequirement::PreserveGlobal)
        .with_file_streams())
    }
}

struct QueryState {
    database: Option<Database>,
    receiver: Option<mpsc::Receiver<RecordBatch>>,
    task: Option<JoinHandle<Result<()>>>,
    interrupt: Arc<InterruptHandle>,
    schema: SchemaRef,
    sql: String,
    day: i32,
    uris: Vec<String>,
    batch_size: usize,
}
impl QueryState {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(database) = self.database.take() {
            let (sender, receiver) = mpsc::channel(1);
            self.receiver = Some(receiver);
            let sql = self.sql.clone();
            let schema = self.schema.clone();
            let uris = self.uris.clone();
            let day = self.day;
            let batch_size = self.batch_size;
            self.task = Some(tokio::task::spawn_blocking(move || {
                if sender.is_closed() {
                    return Ok(());
                }
                let mut statement = database.connection.prepare(&sql).map_err(db_error)?;
                let _ = statement
                    .stream_arrow(params![
                        Value::Date32(day),
                        &uris[0],
                        &uris[1],
                        &uris[2],
                        &uris[3]
                    ])
                    .map_err(db_error)?;
                while !sender.is_closed() {
                    let Some(batch) = statement.step().map_err(db_error)? else {
                        break;
                    };
                    let batch = RecordBatch::from(&batch);
                    let columns = batch
                        .columns()
                        .iter()
                        .zip(schema.fields())
                        .map(|(array, field)| {
                            arrow_cast::cast(array, field.data_type())
                                .map_err(|e| error(e.to_string()))
                        })
                        .collect::<Result<Vec<ArrayRef>>>()?;
                    let batch = RecordBatch::try_new(schema.clone(), columns)
                        .map_err(|e| error(e.to_string()))?;
                    for offset in (0..batch.num_rows()).step_by(batch_size) {
                        if sender
                            .blocking_send(
                                batch.slice(offset, batch_size.min(batch.num_rows() - offset)),
                            )
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
                Ok(())
            }));
        }
        if let Some(batch) = self.receiver.as_mut().expect("query started").recv().await {
            return Ok(Some(batch));
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(|e| {
                Error::new(
                    ErrorKind::Internal,
                    "courtlistener_query_task",
                    e.to_string(),
                )
                .with_source(e)
            })??;
        }
        Ok(None)
    }
}
impl Drop for QueryState {
    fn drop(&mut self) {
        self.receiver.take();
        self.interrupt.interrupt();
    }
}
