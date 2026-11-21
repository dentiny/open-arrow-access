//! Common binary-content rows for PDF, attachment-free LaTeX, and supplied XML shards.
use super::{
    ArxivMetadataIndex, ArxivReadRequest, ArxivRecords, ArxivSourceOptions, error, validation,
};
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use futures::{AsyncReadExt, StreamExt, TryStreamExt, io::Cursor, stream};
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, ObjectRef, ReadOptions,
    RecordBatch, RecordBatchBuilder, RecordCapability, RecordMetadata, RecordReader, RecordStream,
    ResolvedProjection, Result, ScanPartition, ScanPlan, ScanPlanner, SchemaRef, async_trait,
    bytes_reader, into_async_read, into_stream, read_all, record_stream_reader, scan_partition,
};
use open_arrow_access_decompression::{
    Codec, DecodedInput, decoded_reader,
    gzip::Gzip,
    tar::{TarLimits, TarStream},
};
use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

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
                content_batches(batch, format, options.clone(), metadata.clone())
            })
            .try_flatten();
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
        ArxivContentFormat::Latex => expand(batch, options).await?,
        _ => stream::iter([document_batch(&batch, format)]).boxed(),
    };
    Ok(batches
        .map(move |batch| index.attach_content(batch?))
        .boxed())
}

struct ArticleBytes(arrow_array::LargeBinaryArray);

impl AsRef<[u8]> for ArticleBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.value(0)
    }
}

/// Expand one article row into attachment-free LaTeX content batches.
/// Share the input Arrow buffer, decode gzip when present, and sniff one TAR
/// header before choosing a single-file or archive reader. TAR batches are
/// emitted incrementally; consume the stream through EOF to catch trailer errors.
async fn expand(batch: RecordBatch, options: ReadOptions) -> Result<RecordStream> {
    let name = batch["entry_name"].as_binary::<i64>().value(0);
    let article_id = validation::content_article_id(name, ArxivContentFormat::Latex)?;
    let (mut input, source) = latex_input(&batch);
    // Inspect only one TAR header, then replay it into the streaming parser.
    let mut prefix = Vec::with_capacity(validation::TAR_HEADER_SIZE);
    (&mut input)
        .take(validation::TAR_HEADER_SIZE as u64)
        .read_to_end(&mut prefix)
        .await
        .map_err(|e| error::decode("invalid source package").with_source(e))?;
    if !validation::is_tar(&prefix) {
        return expand_single_latex(input, prefix, &batch, &article_id).await;
    }
    let input = decoded_reader(
        Cursor::new(prefix).chain(input),
        ByteMetadata {
            source,
            ..ByteMetadata::default()
        },
        NonZeroUsize::new(64 * 1024).unwrap(),
        "read_arxiv_latex",
        "source",
    );
    let tar = TarStream::new(
        input,
        TarLimits {
            max_entry_bytes: u64::MAX,
            ..TarLimits::default()
        },
    );
    Ok(expand_tar_latex(tar, batch, article_id, options))
}

/// Retain the article's Arrow array while decoding, without copying its payload.
fn latex_input(batch: &RecordBatch) -> (DecodedInput, Option<ObjectRef>) {
    let shard = batch["shard_uri"].as_string::<i64>().value(0);
    let outer_index = batch["entry_index"]
        .as_primitive::<arrow_array::types::UInt64Type>()
        .value(0);
    let source = Some(ObjectRef::new(format!("{shard}#entry={outer_index}")));
    let values = batch["data"].as_binary::<i64>();
    let data = Bytes::from_owner(ArticleBytes(values.clone()));
    let gzip = data.starts_with(&[0x1f, 0x8b]);
    let input = Box::pin(into_async_read(
        Box::new(bytes_reader(data, source.clone())),
        "read_arxiv_latex",
    ));
    let input: DecodedInput = if gzip { Gzip.decoder(input) } else { input };
    (input, source)
}

/// Append the remaining decoded bytes to the sniffed prefix and validate the
/// complete single-file source before emitting one `main.tex` content row.
async fn expand_single_latex(
    mut input: DecodedInput,
    mut data: Vec<u8>,
    batch: &RecordBatch,
    article_id: &str,
) -> Result<RecordStream> {
    input
        .read_to_end(&mut data)
        .await
        .map_err(|e| error::decode("invalid source package").with_source(e))?;
    validation::source_packaging(&data)?;
    let mut builder = RecordBatchBuilder::new(content_schema(), None)?;
    append_latex(&mut builder, batch, article_id, 0, b"main.tex", &data)?;
    Ok(stream::iter([builder.finish()]).boxed())
}

/// Stream batches of non-ancillary TeX members while preserving article provenance.
/// Reject archives with no selected files; decoder and source errors may follow
/// earlier batches, so callers must consume the stream through EOF.
fn expand_tar_latex(
    tar: TarStream,
    batch: RecordBatch,
    article_id: String,
    options: ReadOptions,
) -> RecordStream {
    stream::try_unfold((tar, false), move |(mut tar, emitted)| {
        let batch = batch.clone();
        let article_id = article_id.clone();
        let options = options.clone();
        async move {
            let batch = read_latex_batch(&mut tar, &batch, &article_id, &options).await?;
            if batch.num_rows() == 0 {
                if !emitted {
                    return Err(error::decode(
                        "source package contains no non-ancillary LaTeX files",
                    ));
                }
                Ok(None)
            } else {
                Ok(Some((batch, (tar, true))))
            }
        }
    })
    .boxed()
}

/// Fill one batch up to the row limit or soft selected-TeX byte target.
async fn read_latex_batch(
    tar: &mut TarStream,
    outer: &RecordBatch,
    article_id: &str,
    options: &ReadOptions,
) -> Result<RecordBatch> {
    let target = ArxivSourceOptions::default().target_bytes.get();
    let mut builder = RecordBatchBuilder::new(content_schema(), None)?;
    let mut bytes = 0usize;
    while builder.len() < options.batch_size.get() && (builder.is_empty() || bytes < target) {
        let Some(member) = tar.next_entry().await? else {
            break;
        };
        // Dropping an excluded member drains it on the next header read;
        // no attachment-sized buffer or intermediate Arrow batch is built.
        if !member.entry.is_file || !validation::is_tex(&member.entry.name)? {
            drop(member.reader);
            continue;
        }
        let data = read_all(member.reader, "read_arxiv_tex").await?;
        bytes = bytes.saturating_add(data.len());
        append_latex(
            &mut builder,
            outer,
            article_id,
            member.entry.index as u64,
            &member.entry.name,
            &data,
        )?;
    }
    builder.finish()
}

/// Validate the LaTeX bytes and append one content row, preserving the payload
/// unchanged. Combine shard/article provenance from the single-row `outer` batch
/// with the selected source file's index and name.
fn append_latex(
    builder: &mut RecordBatchBuilder,
    outer: &RecordBatch,
    article_id: &str,
    index: u64,
    name: &[u8],
    data: &[u8],
) -> Result<()> {
    validation::validate_latex_bytes(data)?;
    builder.push(|output| {
        output.append("article_id", article_id)?;
        output.append("format", "latex")?;
        output.append("shard_uri", outer["shard_uri"].as_string::<i64>().value(0))?;
        output.append(
            "entry_index",
            outer["entry_index"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .value(0),
        )?;
        output.append(
            "entry_name",
            outer["entry_name"].as_binary::<i64>().value(0),
        )?;
        output.append("source_index", index)?;
        output.append("source_name", name)?;
        output.append("data", data)?;
        output.append("data_type", ArxivContentFormat::Latex.as_str())
    })
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
