//! Typed streaming readers for the four CourtListener join inputs.
use super::{
    CourtListenerBulkAccessor, CourtListenerSchemas,
    copy_csv::{CopyCsv, CourtListenerCopyDialect},
    validation::parse_object,
    values::{self, error},
};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{Schema, SchemaRef};
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
        let reader = self.bulk.open(ByteReadRequest::from(&request)).await?;
        let mut input = CopyCsv::new(
            reader,
            request.object.locator().into(),
            self.max_record_bytes.get(),
            schema.fields().len(),
            self.dialect,
        );
        let expected = schema
            .fields()
            .iter()
            .map(|f| Some(f.name().clone()))
            .collect::<Vec<_>>();
        if input.row().await?.as_ref() != Some(&expected) {
            return Err(error(format!(
                "{}: missing or mismatched CourtListener CSV header",
                request.object.locator()
            )));
        }
        input.ordinal = 0;
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
        let first_record = self.input.ordinal;
        let mut rows = Vec::new();
        let mut bytes = 0;
        while rows.len() < self.batch_size.min(1024) && bytes < 8 * 1024 * 1024 {
            let Some(row) = self.input.row().await? else {
                break;
            };
            if row.len() != self.schema.fields().len() {
                return Err(error(format!(
                    "{} record {}: CSV column count differs from pinned schema",
                    self.input.object,
                    self.input.ordinal - 1
                )));
            }
            bytes += row.iter().flatten().map(String::len).sum::<usize>();
            rows.push(row);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        let context = format!(
            "{} records {first_record}..{}",
            self.input.object, self.input.ordinal
        );
        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let input = StringArray::from_iter(rows.iter().map(|row| row[i].as_deref()));
                values::cast(&input, field, &context)
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(self.schema.clone(), columns)
            .map(Some)
            .map_err(|e| error(e.to_string()))
    }
}
