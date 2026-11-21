//! Typed streaming readers for the four CourtListener join inputs.
use super::{
    CourtListenerBulkAccessor, CourtListenerSchemas, CourtListenerSnapshot,
    copy_csv::{CopyCsv, CourtListenerCopyDialect},
    validation::parse_object,
    values::{self, Row, error},
};
use arrow_array::{RecordBatch, StructArray};
use arrow_schema::{Fields, Schema, SchemaRef};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, OrderRequirement, ReadRequest, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, async_trait, record_stream_reader,
};
use std::{num::NonZeroUsize, sync::Arc};

/// Typed COPY CSV reader. Its object locator selects one exact table/date CSV.bz2.
pub struct CourtListenerTableAccessor<A> {
    bulk: CourtListenerBulkAccessor<A>,
    schemas: CourtListenerSchemas,
    max_record_bytes: NonZeroUsize,
    dialect: CourtListenerCopyDialect,
}
impl<A> Clone for CourtListenerTableAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            bulk: self.bulk.clone(),
            schemas: self.schemas.clone(),
            max_record_bytes: self.max_record_bytes,
            dialect: self.dialect,
        }
    }
}
impl<A> CourtListenerTableAccessor<A> {
    pub fn new(storage: A) -> Self {
        Self {
            bulk: CourtListenerBulkAccessor::new(storage),
            schemas: CourtListenerSchemas::default(),
            max_record_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
            dialect: CourtListenerCopyDialect::default(),
        }
    }
    pub fn with_schemas(mut self, schemas: CourtListenerSchemas) -> Result<Self> {
        schemas.validate()?;
        self.schemas = schemas;
        Ok(self)
    }
    pub fn with_max_record_bytes(mut self, limit: NonZeroUsize) -> Self {
        self.max_record_bytes = limit;
        self
    }
    /// Select a reviewed historical COPY quote/escape pair.
    pub fn with_dialect(mut self, dialect: CourtListenerCopyDialect) -> Result<Self> {
        dialect.validate()?;
        self.dialect = dialect;
        Ok(self)
    }
    /// A table's pinned header fields, typed and ordered as emitted.
    pub fn table_schema(&self, table: &str) -> Result<SchemaRef> {
        let fields = match table {
            "opinions" => &self.schemas.opinion,
            "opinion-clusters" => &self.schemas.cluster,
            "dockets" => &self.schemas.docket,
            "courts" => &self.schemas.court,
            _ => return Err(super::error::invalid()),
        };
        Ok(Arc::new(Schema::new(fields.clone())))
    }
}

pub(super) async fn open_csv<A: ByteAccessor>(
    bulk: &CourtListenerBulkAccessor<A>,
    snapshot: CourtListenerSnapshot,
    request: &ReadRequest,
    fields: &Fields,
    uri: String,
    framing: (usize, CourtListenerCopyDialect),
) -> Result<CopyCsv> {
    let reader = bulk
        .open(ByteReadRequest::new(snapshot.object()).with_options(request.byte_options))
        .await?;
    let mut input = CopyCsv::new(reader, uri.clone(), framing.0, fields.len(), framing.1);
    let header = input
        .row()
        .await?
        .ok_or_else(|| error(format!("{uri}: empty upstream export")))?;
    let expected = fields
        .iter()
        .map(|f| Some(f.name().clone()))
        .collect::<Vec<_>>();
    if header != expected {
        return Err(error(format!(
            "{uri}: header differs from explicitly selected schema"
        )));
    }
    input.ordinal = 0;
    Ok(input)
}
pub(super) async fn typed(input: &mut CopyCsv, fields: &Fields) -> Result<Option<(Row, u64)>> {
    let index = input.ordinal;
    let context = format!("{} record {index}", input.object);
    input
        .row()
        .await?
        .map(|raw| values::decode(raw, fields, &context).map(|row| (row, index)))
        .transpose()
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for CourtListenerTableAccessor<A> {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.schemas.validate()?;
        let object = parse_object(request.object.locator()).ok_or_else(super::error::invalid)?;
        let schema = self.table_schema(object.table)?;
        let projection = request.options.resolve_projection(&schema)?;
        let input = open_csv(
            &self.bulk,
            CourtListenerSnapshot::new(object.table, object.date)?,
            &request,
            schema.fields(),
            request.object.locator().into(),
            (self.max_record_bytes.get(), self.dialect),
        )
        .await?;
        let metadata = RecordMetadata {
            schema: projection.schema.clone(),
            source: Some(request.object.clone()),
        };
        let state = TableState {
            input,
            schema,
            batch_size: request.options.batch_size.get(),
        };
        let batches = stream::try_unfold(state, |mut state| async move {
            let Some(batch) = state.next().await? else {
                return Ok(None);
            };
            Ok(Some((batch, state)))
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
impl<A: ByteAccessor + 'static> ScanPlanner for CourtListenerTableAccessor<A> {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let object = parse_object(request.object.locator()).ok_or_else(super::error::invalid)?;
        let metadata = RecordMetadata {
            schema: request
                .options
                .resolve_projection(&self.table_schema(object.table)?)?
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
struct TableState {
    input: CopyCsv,
    schema: SchemaRef,
    batch_size: usize,
}
impl TableState {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        let mut rows = Vec::new();
        let mut bytes = 0;
        while rows.len() < self.batch_size.min(1024) && bytes < 8 * 1024 * 1024 {
            let Some((row, _)) = typed(&mut self.input, self.schema.fields()).await? else {
                break;
            };
            bytes += row
                .iter()
                .map(|v| v.as_str().map_or(8, str::len))
                .sum::<usize>();
            rows.push(row);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        let array = values::structure(
            self.schema.fields(),
            &rows.iter().map(Some).collect::<Vec<_>>(),
        )?;
        let array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("structure output");
        RecordBatch::try_new(self.schema.clone(), array.columns().to_vec())
            .map(Some)
            .map_err(|e| error(e.to_string()))
    }
}
