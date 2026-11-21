//! Import typed Arrow batches into DuckDB, SQL-join a date, stream Arrow results.
use super::{
    CourtListenerCopyDialect, CourtListenerSchemas, CourtListenerSnapshot,
    query::query,
    source::{TABLES, ingest},
    valdiation::{validate_join_options, validate_join_schema},
};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, OrderRequirement, ReadRequest, RecordCapability, RecordMetadata,
    RecordReader, Result, ScanPlan, ScanPlanner, async_trait, record_stream_reader,
};
use std::{path::PathBuf, sync::Arc};

/// Caller-owned scratch and provenance for one date conversion.
#[derive(Clone, Debug)]
pub struct CourtListenerJoinOptions {
    pub source_root: String,
    pub scratch_directory: PathBuf,
    pub dialect: CourtListenerCopyDialect,
}
impl CourtListenerJoinOptions {
    pub fn new(source_root: impl Into<String>, scratch_directory: PathBuf) -> Self {
        Self {
            source_root: source_root.into(),
            scratch_directory,
            dialect: Default::default(),
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
        validate_join_options(&options)?;
        Ok(Self {
            storage: Arc::new(storage),
            options,
            schemas: Default::default(),
        })
    }
    pub fn with_schemas(mut self, schemas: CourtListenerSchemas) -> Result<Self> {
        validate_join_schema(&schemas)?;
        self.schemas = schemas;
        Ok(self)
    }
    pub fn schemas(&self) -> &CourtListenerSchemas {
        &self.schemas
    }
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
        let day = request.object.locator().to_owned();
        let database = ingest(
            self.storage.clone(),
            self.options.clone(),
            self.schemas.clone(),
            &request,
        )
        .await?;
        let uris = TABLES
            .iter()
            .map(|table| {
                let object = CourtListenerSnapshot::new(*table, &day)?.object();
                Ok(format!(
                    "{}/{}",
                    self.options.source_root.trim_end_matches('/'),
                    object.locator()
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let state = query(
            database,
            &self.schemas,
            day,
            uris,
            request.options.batch_size.get(),
        );
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
