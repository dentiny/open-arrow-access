//! Common binary-content rows for PDF, attachment-free LaTeX, and supplied XML shards.
use super::{ArxivMetadataIndex, ArxivReadRequest, ArxivRecords, validation};
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadOptions, RecordBatch, RecordBatchBuilder,
    RecordCapability, RecordMetadata, RecordReader, RecordStream, ResolvedProjection, Result,
    ScanPartition, ScanPlan, ScanPlanner, SchemaRef, async_trait, into_stream,
    record_stream_reader, scan_partition,
};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArxivContentFormat {
    Pdf,
    Latex,
    Xml,
}
impl ArxivContentFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Latex => "latex",
            Self::Xml => "xml",
        }
    }
}

pub fn content_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let metadata = crate::arxiv_metadata::schema().arrow_schema();
            Arc::new(Schema::new(vec![
                Field::new("article_id", DataType::LargeUtf8, false),
                Field::new("format", DataType::LargeUtf8, false),
                Field::new("shard_uri", DataType::LargeUtf8, false),
                Field::new("entry_index", DataType::UInt64, false),
                Field::new("entry_name", DataType::LargeBinary, false),
                Field::new("source_index", DataType::UInt64, false),
                Field::new("source_name", DataType::LargeBinary, false),
                Field::new("data", DataType::LargeBinary, false),
                Field::new("data_type", DataType::LargeUtf8, false),
                Field::new(
                    "metadata",
                    DataType::Struct(metadata.fields().clone()),
                    true,
                ),
            ]))
        })
        .clone()
}

#[derive(Clone)]
enum ContentMetadata {
    Online(Arc<super::metadata_fetch::MetadataResolver>),
    Snapshot(Arc<ArxivMetadataIndex>),
}
impl ContentMetadata {
    async fn resolve(&self, article: &str) -> Result<Arc<ArxivMetadataIndex>> {
        match self {
            Self::Online(resolver) => resolver.get(article).await,
            Self::Snapshot(index) => Ok(index.clone()),
        }
    }
}

pub struct ArxivContentRecords<A> {
    format: ArxivContentFormat,
    records: ArxivRecords<A>,
    metadata: ContentMetadata,
}
impl<A> ArxivRecords<A> {
    /// Read content with complete OAI metadata, fetched lazily and cached by article ID.
    pub fn content(self, format: ArxivContentFormat) -> ArxivContentRecords<A> {
        let metadata = ContentMetadata::Online(self.service.metadata.clone());
        ArxivContentRecords {
            records: self,
            format,
            metadata,
        }
    }
}
impl<A> ArxivContentRecords<A> {
    /// Use a shared typed OAI snapshot instead of fetching metadata automatically.
    /// Missing article matches fail the scan, including when metadata is projected
    /// away. No metadata network requests are made by content workers.
    pub fn with_metadata(mut self, metadata: Arc<ArxivMetadataIndex>) -> Result<Self> {
        validation::validate_content_schema(&metadata)?;
        self.metadata = ContentMetadata::Snapshot(metadata);
        Ok(self)
    }
}
#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> Accessor
    for ArxivContentRecords<A>
{
    type Request = ArxivReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        self.records.capability()
    }
    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}
#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> ScanPlanner
    for ArxivContentRecords<A>
{
    async fn plan_scan(&self, mut request: Self::Request) -> Result<ScanPlan> {
        validation::validate_content_kind(self.format, request.kind)?;
        let schema = content_schema();
        let projection = request.options.resolve_projection(&schema)?;
        let options = request.options.clone();
        request.options.projection = None;
        request.options.batch_size = std::num::NonZeroUsize::MIN;
        let mut plan = match request.shard.clone() {
            Some(shard) => self.records.plan_shard_entries(shard, request)?,
            None => self.records.plan_manifest_entries(request).await?,
        };
        plan.metadata.schema = projection.schema.clone();
        let format = self.format;
        // Only article partitions can be skipped without dropping other articles.
        let article_partitions = plan.streaming_discovery;
        let metadata = self.metadata.clone();
        plan.partitions = plan
            .partitions
            .map(move |partition| {
                partition.map(|partition| {
                    content_partition(
                        partition,
                        format,
                        projection.clone(),
                        options.clone(),
                        metadata.clone(),
                        article_partitions,
                    )
                })
            })
            .boxed();
        Ok(plan)
    }
}

fn content_partition(
    partition: ScanPartition,
    format: ArxivContentFormat,
    projection: ResolvedProjection,
    options: ReadOptions,
    metadata: ContentMetadata,
    article_partition: bool,
) -> ScanPartition {
    scan_partition(async move {
        let reader = partition.await?;
        let ordering = reader.capability().ordering;
        let output = RecordMetadata {
            schema: projection.schema,
            source: reader.metadata().source.clone(),
        };
        let batches = into_stream(reader)
            .and_then(move |batch| {
                content_batches(batch, format, options.clone(), metadata.clone())
            })
            .try_flatten()
            .map_err(move |error| {
                if !article_partition && error.operation() == "read_arxiv_latex"
                    && error.kind() == open_arrow_access_core::ErrorKind::NotFound
                {
                    // A file partition can contain other valid articles. Never
                    // let a missing-object policy discard the whole TAR.
                    super::error::decode("TAR contains an article without LaTeX; select a single shard for article-level skipping")
                        .with_object(error.object()).with_source(error)
                } else {
                    error
                }
            });
        Ok(record_stream_reader(
            batches,
            output,
            projection.indices,
            ordering,
        ))
    })
}

async fn content_batches(
    batch: RecordBatch,
    format: ArxivContentFormat,
    options: ReadOptions,
    metadata: ContentMetadata,
) -> Result<RecordStream> {
    let article =
        validation::content_article_id(batch["entry_name"].as_binary::<i64>().value(0), format)?;
    let index = metadata.resolve(&article).await?;
    let batches = match format {
        ArxivContentFormat::Pdf => super::pdf::read(batch)?,
        ArxivContentFormat::Latex => super::latex::read(batch, options).await?,
        ArxivContentFormat::Xml => super::xml::read(batch)?,
    };
    Ok(batches
        .map(move |batch| index.attach_content(batch?))
        .boxed())
}

pub(super) fn document_batch(
    batch: &RecordBatch,
    format: ArxivContentFormat,
) -> Result<RecordBatch> {
    let name = batch["entry_name"].as_binary::<i64>().value(0);
    let id = validation::content_article_id(name, format)?;
    let mut builder = RecordBatchBuilder::new(content_schema(), None)?;
    builder.push(|row| {
        row.append("article_id", id.as_str())?;
        row.append("format", format.as_str())?;
        row.append("shard_uri", batch["shard_uri"].as_string::<i64>().value(0))?;
        row.append(
            "entry_index",
            batch["entry_index"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(0),
        )?;
        row.append("entry_name", name)?;
        row.append("source_index", 0_u64)?;
        row.append("source_name", b"".as_slice())?;
        row.append("data", batch["data"].as_binary::<i64>().value(0))?;
        row.append("data_type", format.as_str())
    })?;
    builder.finish()
}

#[cfg(test)]
mod tests;
