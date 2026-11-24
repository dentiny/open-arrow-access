//! Build an immutable local database once and read date joins without CSV storage.
use super::{
    CourtListenerCopyDialect, CourtListenerDocumentAccessor, CourtListenerSchemas,
    CourtListenerSnapshot,
    error::{db_error, records_error, task_error},
    ingest::{self, DB_TABLES, Database},
    query::query,
    validation::{
        validate_database_contract, validate_database_date, validate_database_snapshot_count,
        validate_database_tables, validate_join_schema,
    },
};
use crate::duckdb::ManagedConnection;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, Error, ErrorKind, OrderRequirement, ReadRequest, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, async_trait, record_stream_reader,
};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

const VERSION: i32 = 1;

/// The exact date/source identity and table counts stored in a local database.
/// Counts describe the supplied inputs, which may be explicitly sampled exports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CourtListenerDatabaseInfo {
    pub snapshot_date: String,
    pub source_root: String,
    /// opinions, opinion_clusters, dockets, courts, in that order.
    pub row_counts: [u64; 4],
}

/// Read a prepared local database, with no byte accessor or CSV import.
#[derive(Clone)]
pub struct CourtListenerDatabaseAccessor {
    path: PathBuf,
    info: CourtListenerDatabaseInfo,
    schemas: CourtListenerSchemas,
}

fn io(error: std::io::Error) -> Error {
    Error::new(ErrorKind::Io, "courtlistener_database", error.to_string()).with_source(error)
}

fn inspect(
    connection: &ManagedConnection,
    schemas: &CourtListenerSchemas,
) -> Result<CourtListenerDatabaseInfo> {
    let rows: u64 = connection
        .query_row("SELECT count(*) FROM courtlistener_snapshot", [], |r| {
            r.get(0)
        })
        .map_err(db_error)?;
    validate_database_snapshot_count(rows)?;
    let (version, date, root, stored): (i32, String, String, String) = connection.query_row(
        "SELECT version, snapshot_date, source_root, schema_contract FROM courtlistener_snapshot", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    ).map_err(db_error)?;
    validate_database_contract(version, VERSION, &stored, &schemas.database_contract())?;
    CourtListenerSnapshot::new("opinions", &date)?;
    super::validation::validate_source_root(&root)?;
    validate_database_tables(connection, schemas)?;
    let mut row_counts = [0; 4];
    for (i, name) in DB_TABLES.iter().enumerate() {
        row_counts[i] = connection.table_row_count(name)?;
    }
    Ok(CourtListenerDatabaseInfo {
        snapshot_date: date,
        source_root: root,
        row_counts,
    })
}

impl CourtListenerDatabaseAccessor {
    /// Open a local file read-only using the pinned default source schema.
    /// All blocking DuckDB work runs off the async executor. No extensions/network.
    pub async fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_path_with_schemas(path, CourtListenerSchemas::default()).await
    }
    /// Open a reviewed historical schema without inferring or migrating columns.
    pub async fn from_path_with_schemas(
        path: impl AsRef<Path>,
        schemas: CourtListenerSchemas,
    ) -> Result<Self> {
        validate_join_schema(&schemas)?;
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || {
            let path = path.canonicalize().map_err(io)?;
            let connection = ManagedConnection::open(&path, true)?;
            let info = inspect(&connection, &schemas)?;
            connection.close()?;
            Ok(Self {
                path,
                info,
                schemas,
            })
        })
        .await
        .map_err(|e| task_error("courtlistener_database_open", e))?
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn info(&self) -> &CourtListenerDatabaseInfo {
        &self.info
    }
    pub fn schemas(&self) -> &CourtListenerSchemas {
        &self.schemas
    }
    pub fn with_document_urls(
        self,
        media_root: impl AsRef<str>,
    ) -> Result<CourtListenerDocumentAccessor<Self>> {
        let schemas = self.schemas.clone();
        CourtListenerDocumentAccessor::new(self, schemas, media_root)
    }
}

pub(super) async fn create<A: ByteAccessor + 'static>(
    storage: Arc<A>,
    dialect: CourtListenerCopyDialect,
    schemas: CourtListenerSchemas,
    request: ReadRequest,
    source_root: String,
    destination: PathBuf,
) -> Result<CourtListenerDatabaseAccessor> {
    let snapshot = CourtListenerSnapshot::new("opinions", request.object.locator())?;
    validate_join_schema(&schemas)?;
    let destination = std::path::absolute(destination).map_err(io)?;
    if destination.try_exists().map_err(io)? {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "courtlistener_database",
            "destination already exists; refusing to overwrite",
        ));
    }
    // Stage on the destination filesystem. Only publish after every importer,
    // uniqueness check, manifest write, checkpoint and close has succeeded.
    let parent = destination
        .parent()
        .ok_or_else(|| super::error::schema_error("database destination must name a file"))?;
    let directory = tempfile::tempdir_in(parent).map_err(io)?;
    let staging = directory.path().join("snapshot.duckdb");
    let staging_open = staging.clone();
    let connection =
        tokio::task::spawn_blocking(move || ManagedConnection::open(staging_open, false))
            .await
            .map_err(|e| task_error("courtlistener_database_create", e))??;
    let database = ingest::ingest(
        storage,
        dialect,
        schemas.clone(),
        &request,
        Some(connection),
    )
    .await?;
    let day = snapshot.date().to_owned();
    let schema_contract = schemas.database_contract();
    let target = destination.clone();
    tokio::task::spawn_blocking(move || {
        database
            .connection
            .execute_batch(
                "CREATE TABLE courtlistener_snapshot (
                version INTEGER NOT NULL, snapshot_date VARCHAR NOT NULL,
                source_root VARCHAR NOT NULL, schema_contract VARCHAR NOT NULL
             )",
            )
            .map_err(db_error)?;
        database
            .connection
            .execute(
                "INSERT INTO courtlistener_snapshot VALUES (?, ?, ?, ?)",
                ::duckdb::params![VERSION, day, source_root, schema_contract],
            )
            .map_err(db_error)?;
        database
            .connection
            .execute_batch("CHECKPOINT")
            .map_err(db_error)?;
        database.connection.close()?;
        // Atomic no-clobber publication; an existing file is never replaced.
        std::fs::hard_link(&staging, &target).map_err(io)?;
        Ok::<_, Error>(())
    })
    .await
    .map_err(|e| task_error("courtlistener_database_publish", e))??;
    drop(directory);
    CourtListenerDatabaseAccessor::from_path_with_schemas(destination, schemas).await
}

#[async_trait]
impl Accessor for CourtListenerDatabaseAccessor {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }
    async fn metadata(&self, request: ReadRequest) -> Result<RecordMetadata> {
        validate_database_date(request.object.locator(), &self.info.snapshot_date)?;
        Ok(RecordMetadata {
            schema: request
                .options
                .resolve_projection(&self.schemas.schema())?
                .schema,
            source: Some(request.object),
        })
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let metadata = self.metadata(request.clone()).await?;
        let projection = request.options.resolve_projection(&self.schemas.schema())?;
        let source = self.clone();
        let database = tokio::task::spawn_blocking(move || {
            let connection = ManagedConnection::open(&source.path, true)?;
            if inspect(&connection, &source.schemas)? != source.info {
                return Err(records_error("database snapshot identity changed"));
            }
            Ok(Database { connection })
        })
        .await
        .map_err(|e| task_error("courtlistener_database_read", e))??;
        let uris = ingest::TABLES
            .iter()
            .map(|table| {
                let object = CourtListenerSnapshot::new(*table, &self.info.snapshot_date)?.object();
                Ok(format!(
                    "{}/{}",
                    self.info.source_root.trim_end_matches('/'),
                    object.locator()
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let state = query(
            database,
            &self.schemas,
            self.info.snapshot_date.clone(),
            uris,
            request.options.batch_size.get(),
        );
        let batches = stream::try_unfold(state, |mut state| async move {
            Ok(state.next().await?.map(|batch| (batch, state)))
        });
        Ok(Box::new(record_stream_reader(
            batches,
            metadata,
            projection.indices,
            OrderRequirement::PreserveGlobal,
        )))
    }
}

#[async_trait]
impl ScanPlanner for CourtListenerDatabaseAccessor {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let metadata = self.metadata(request.clone()).await?;
        let source = self.clone();
        Ok(ScanPlan::new(
            metadata,
            vec![Box::pin(async move { source.open(request).await })],
        )
        .with_ordering(OrderRequirement::PreserveGlobal)
        .with_file_streams())
    }
}
