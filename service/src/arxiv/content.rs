//! Common binary-content rows for PDF, attachment-free LaTeX, and supplied XML shards.
use super::{ArxivReadRequest, ArxivRecords, ArxivSourceOptions, error, validation};
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

pub struct ArxivContentRecords<A> {
    format: ArxivContentFormat,
    records: ArxivRecords<A>,
}
impl<A> ArxivRecords<A> {
    pub fn content(self, format: ArxivContentFormat) -> ArxivContentRecords<A> {
        ArxivContentRecords {
            records: self,
            format,
        }
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
        plan.partitions = plan
            .partitions
            .map(move |partition| {
                partition.map(|partition| {
                    content_partition(partition, format, projection.clone(), options.clone())
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
                async move {
                    match format {
                        ArxivContentFormat::Latex => expand(batch, options).await,
                        _ => Ok(stream::iter([document_batch(&batch, format)]).boxed()),
                    }
                }
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

struct ArticleBytes(arrow_array::LargeBinaryArray);

impl AsRef<[u8]> for ArticleBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.value(0)
    }
}

async fn expand(batch: RecordBatch, options: ReadOptions) -> Result<RecordStream> {
    let name = batch["entry_name"].as_binary::<i64>().value(0);
    let article_id = validation::content_article_id(name, ArxivContentFormat::Latex)?;
    let shard = batch["shard_uri"].as_string::<i64>().value(0);
    let outer_index = batch["entry_index"]
        .as_primitive::<arrow_array::types::UInt64Type>()
        .value(0);
    let source = Some(ObjectRef::new(format!("{shard}#entry={outer_index}")));
    let values = batch["data"].as_binary::<i64>();
    // Retain the Arrow array instead of copying the compressed article package.
    let data = Bytes::from_owner(ArticleBytes(values.clone()));
    let gzip = data.starts_with(&[0x1f, 0x8b]);
    let input = Box::pin(into_async_read(
        Box::new(bytes_reader(data, source.clone())),
        "read_arxiv_latex",
    ));
    let mut input: DecodedInput = if gzip { Gzip.decoder(input) } else { input };
    // Inspect only one TAR header, then replay it into the streaming parser.
    let mut prefix = Vec::with_capacity(512);
    (&mut input)
        .take(512)
        .read_to_end(&mut prefix)
        .await
        .map_err(|e| error::decode("invalid source package").with_source(e))?;
    if !validation::is_tar(&prefix) {
        input
            .read_to_end(&mut prefix)
            .await
            .map_err(|e| error::decode("invalid source package").with_source(e))?;
        validation::source_packaging(&prefix)?;
        let mut builder = RecordBatchBuilder::new(content_schema(), None)?;
        append_latex(&mut builder, &batch, &article_id, 0, b"main.tex", &prefix)?;
        return Ok(stream::iter([builder.finish()]).boxed());
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
    let target = ArxivSourceOptions::default().target_bytes.get();
    Ok(stream::try_unfold((tar, false), move |(mut tar, emitted)| {
        let batch = batch.clone();
        let article_id = article_id.clone();
        let options = options.clone();
        async move {
            let mut builder = RecordBatchBuilder::new(content_schema(), None)?;
            let mut bytes = 0usize;
            while builder.len() < options.batch_size.get() && (builder.is_empty() || bytes < target)
            {
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
                    &batch,
                    &article_id,
                    member.entry.index as u64,
                    &member.entry.name,
                    &data,
                )?;
            }
            if builder.is_empty() {
                if !emitted {
                    return Err(error::decode(
                        "source package contains no non-ancillary LaTeX files",
                    ));
                }
                Ok(None)
            } else {
                Ok(Some((builder.finish()?, (tar, true))))
            }
        }
    })
    .boxed())
}

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
