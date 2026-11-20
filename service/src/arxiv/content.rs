//! Common binary-content rows for PDF, attachment-free LaTeX, and supplied XML shards.
use super::{
    ArxivMetadataIndex, ArxivReadRequest, ArxivRecords, ArxivSourceOptions, error,
    open_source_package, validation,
};
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ObjectRef, ReadOptions, RecordBatch,
    RecordBatchBuilder, RecordCapability, RecordMetadata, RecordReader, ResolvedProjection, Result,
    ScanPartition, ScanPlan, ScanPlanner, SchemaRef, async_trait, bytes_reader, into_async_read,
    into_stream, record_stream_reader, scan_partition,
};
use open_arrow_access_decompression::{Codec, gzip::Gzip};
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
        metadata.validate_content_schema()?;
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
            None => self.records.plan_scan(request).await?,
        };
        plan.metadata.schema = projection.schema.clone();
        let format = self.format;
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
                let options = options.clone();
                let metadata = metadata.clone();
                async move {
                    let article = validation::content_article_id(
                        batch["entry_name"].as_binary::<i64>().value(0),
                        format,
                    )?;
                    let index = metadata.resolve(&article).await?;
                    let batches = match format {
                        ArxivContentFormat::Latex => expand(&batch, &options).await,
                        _ => Ok(vec![document_batch(&batch, format)?]),
                    }?;
                    batches
                        .into_iter()
                        .map(|batch| index.attach_content(batch))
                        .collect::<Result<Vec<_>>>()
                }
            })
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(record_stream_reader(
            batches,
            output,
            projection.indices,
            ordering,
        ))
    })
}

async fn expand(batch: &RecordBatch, options: &ReadOptions) -> Result<Vec<RecordBatch>> {
    let name = batch["entry_name"].as_binary::<i64>().value(0);
    let article_id = validation::content_article_id(name, ArxivContentFormat::Latex)?;
    let shard = batch["shard_uri"].as_string::<i64>().value(0);
    let outer_index = batch["entry_index"]
        .as_primitive::<arrow_array::types::UInt64Type>()
        .value(0);
    let mut data = batch["data"].as_binary::<i64>().value(0).to_vec();
    if data.starts_with(&[0x1f, 0x8b]) {
        use futures::AsyncReadExt;
        let input = bytes_reader(Bytes::from(data), None);
        let mut decoder = Gzip.decoder(Box::pin(into_async_read(
            Box::new(input),
            "decode_arxiv_latex",
        )));
        data = Vec::new();
        decoder
            .read_to_end(&mut data)
            .await
            .map_err(|e| error::decode("invalid gzip source package").with_source(e))?;
    }
    let packaging = validation::source_packaging(&data)?;
    let input = bytes_reader(
        Bytes::from(data),
        Some(ObjectRef::new(format!("{shard}#entry={outer_index}"))),
    );
    let mut reader = open_source_package(
        Box::new(input),
        &article_id,
        packaging,
        ArxivSourceOptions {
            options: ReadOptions {
                projection: None,
                ..options.clone()
            },
            ..ArxivSourceOptions::default()
        },
    )
    .await?;
    let mut batches = Vec::new();
    while let Some(member_batch) = reader.next().await? {
        let batch = latex_batch(batch, &article_id, &member_batch)?;
        if batch.num_rows() != 0 {
            batches.push(batch);
        }
    }
    validation::validate_latex_files(&batches)?;
    Ok(batches)
}

fn latex_batch(outer: &RecordBatch, article_id: &str, batch: &RecordBatch) -> Result<RecordBatch> {
    let shard = outer["shard_uri"].as_string::<i64>().value(0);
    let outer_index = outer["entry_index"]
        .as_primitive::<arrow_array::types::UInt64Type>()
        .value(0);
    let name = outer["entry_name"].as_binary::<i64>().value(0);
    let mut builder = RecordBatchBuilder::new(content_schema(), None)?;
    let names = batch["entry_name"].as_binary::<i64>();
    let data = batch["data"].as_binary::<i64>();
    let indices = batch["entry_index"].as_primitive::<arrow_array::types::UInt64Type>();
    for row in 0..batch.num_rows() {
        let source_name = names.value(row);
        if !validation::is_tex(source_name)? {
            continue;
        }
        validation::validate_latex_bytes(data.value(row))?;
        builder.push(|output| {
            output.append("article_id", article_id)?;
            output.append("format", "latex")?;
            output.append("shard_uri", shard)?;
            output.append("entry_index", outer_index)?;
            output.append("entry_name", name)?;
            output.append("source_index", indices.value(row))?;
            output.append("source_name", source_name)?;
            output.append("data", data.value(row))?;
            output.append("data_type", ArxivContentFormat::Latex.as_str())
        })?;
    }
    builder.finish()
}

fn document_batch(batch: &RecordBatch, format: ArxivContentFormat) -> Result<RecordBatch> {
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
