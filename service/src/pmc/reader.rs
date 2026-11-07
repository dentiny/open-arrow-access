use super::{
    PmcLimits, capability, decode,
    input::{JsonInput, read_bounded},
    metadata, schema, validation,
};
use arrow_array::{ArrayRef, builder::LargeStringBuilder};
use arrow_schema::Schema;
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, Capability, ObjectStream, ReadOptions, ReadRequest,
    Reader, RecordBatch, RecordMetadata, Result, async_trait,
};
use open_arrow_access_file_format::JsonAccessor;
use std::{num::NonZeroUsize, sync::Arc};

pub(super) struct PmcReader<A> {
    pub(super) accessor: Arc<A>,
    pub(super) objects: Option<ObjectStream>,
    pub(super) options: ReadOptions,
    pub(super) byte_options: ByteReadOptions,
    pub(super) limits: PmcLimits,
    pub(super) projection: Option<Vec<usize>>,
    pub(super) metadata: RecordMetadata,
}

impl<A: ByteAccessor> PmcReader<A> {
    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut rows = 0;
        let mut payload_bytes = 0usize;
        let mut normalized = Vec::new();
        // Metadata objects can reach the payload target before the row target.
        let mut raw_json = LargeStringBuilder::new();
        while rows < self.options.batch_size.get() && payload_bytes < self.limits.batch_bytes.get()
        {
            let Some(object) = self.objects.as_mut().unwrap().try_next().await? else {
                self.objects = None;
                break;
            };
            let raw = read_bounded(
                self.accessor.as_ref(),
                object.clone(),
                self.byte_options,
                self.limits.metadata_bytes.get(),
            )
            .await?;
            let Some(mapped) = metadata::parse(&raw, object.locator())? else {
                continue;
            };
            validation::validate_pdf_url(&mapped.source_uri)?;
            let before = normalized.len();
            serde_json::to_writer(&mut normalized, &mapped)
                .map_err(|error| decode(error.to_string()).with_source(error))?;
            normalized.push(b'\n');
            payload_bytes = payload_bytes
                .saturating_add(raw.len())
                .saturating_add(normalized.len() - before);
            // parse() already validated UTF-8, but preserve the complete original
            // object, including whitespace, unknown fields and duplicate keys.
            raw_json.append_value(
                std::str::from_utf8(&raw)
                    .map_err(|error| decode(error.to_string()).with_source(error))?,
            );
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }

        // Reuse our JSON -> Arrow format path for every scalar metadata column.
        // Only verbatim JSON needs a dedicated array.
        let full_schema = schema();
        let json_schema = Arc::new(Schema::new(full_schema.fields()[..7].to_vec()));
        let mut json_reader = JsonAccessor::new(JsonInput(normalized.into()), json_schema)
            .open(
                ReadRequest::new("pmc-normalized-metadata").with_options(ReadOptions {
                    batch_size: NonZeroUsize::new(rows).unwrap(),
                    projection: None,
                }),
            )
            .await?;
        let batch = json_reader
            .next()
            .await?
            .ok_or_else(|| decode("missing normalized metadata batch"))?;
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(raw_json.finish()) as ArrayRef);
        let batch = RecordBatch::try_new(full_schema, columns)
            .map_err(|error| decode(error.to_string()).with_source(error))?;
        let batch = if let Some(indices) = &self.projection {
            batch
                .project(indices)
                .map_err(|error| decode(error.to_string()).with_source(error))?
        } else {
            batch
        };
        Ok(Some(batch))
    }
}

#[async_trait]
impl<A: ByteAccessor> Reader for PmcReader<A> {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    fn capability(&self) -> Capability {
        capability(true)
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.objects.is_none() {
            return Ok(None);
        }
        let result = self.read_batch().await;
        if !matches!(result, Ok(Some(_))) {
            self.objects = None;
        }
        result
    }
}
