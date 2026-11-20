//! LaTeX-only bulk source records; ancillary assets never enter the output.
use super::{
    ArxivKind, ArxivMetadataIndex, ArxivReadRequest, ArxivRecords, ArxivSourceOptions,
    ArxivSourcePackaging, error, id, open_source_package,
};
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, ObjectRef, ReadOptions, RecordBatch,
    RecordBatchBuilder, RecordCapability, RecordMetadata, RecordReader, ResolvedProjection, Result,
    ScanPartition, ScanPlan, ScanPlanner, SchemaRef, StreamReader, async_trait, bytes_reader,
    into_async_read, project_batch, scan_partition,
};
use open_arrow_access_decompression::{Codec, gzip::Gzip};
use std::sync::{Arc, OnceLock};

pub fn latex_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("article_id", DataType::LargeUtf8, false),
                Field::new("format", DataType::LargeUtf8, false),
                Field::new("shard_uri", DataType::LargeUtf8, false),
                Field::new("entry_index", DataType::UInt64, false),
                Field::new("entry_name", DataType::LargeBinary, false),
                Field::new("source_index", DataType::UInt64, false),
                Field::new("source_name", DataType::LargeBinary, false),
                Field::new("text", DataType::LargeUtf8, false),
            ]))
        })
        .clone()
}

pub struct ArxivLatexRecords<A> {
    records: ArxivRecords<A>,
    metadata: Option<Arc<ArxivMetadataIndex>>,
}
impl<A> ArxivRecords<A> {
    /// Expand source packages to UTF-8 `.tex` files outside `anc/`. Each file is
    /// a row; no concatenation, compilation, include expansion or PDF fallback.
    /// Sources without TeX, invalid UTF-8 and corrupt archives fail the scan.
    pub fn latex(self) -> ArxivLatexRecords<A> {
        ArxivLatexRecords {
            records: self,
            metadata: None,
        }
    }
}
impl<A> ArxivLatexRecords<A> {
    pub fn with_metadata(mut self, metadata: Arc<ArxivMetadataIndex>) -> Self {
        self.metadata = Some(metadata);
        self
    }
}
#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> Accessor
    for ArxivLatexRecords<A>
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
    for ArxivLatexRecords<A>
{
    async fn plan_scan(&self, mut request: Self::Request) -> Result<ScanPlan> {
        if request.kind != ArxivKind::Source {
            return Err(error::invalid("LaTeX records require kind=source"));
        }
        let schema = self
            .metadata
            .as_ref()
            .map_or_else(latex_schema, |index| index.schema_for(&latex_schema()));
        let projection = request.options.resolve_projection(&schema)?;
        let options = request.options.clone();
        request.options.projection = None;
        request.options.batch_size = std::num::NonZeroUsize::MIN;
        let mut plan = self.records.plan_scan(request).await?;
        plan.metadata.schema = projection.schema.clone();
        let index = self.metadata.clone();
        plan.partitions = plan
            .partitions
            .map(move |partition| {
                partition.map(|partition| {
                    latex_partition(
                        partition,
                        projection.clone(),
                        options.clone(),
                        index.clone(),
                    )
                })
            })
            .boxed();
        Ok(plan)
    }
}

struct LatexScanState {
    reader: Box<dyn RecordReader>,
    pending: std::vec::IntoIter<RecordBatch>,
    projection: ResolvedProjection,
    options: ReadOptions,
    index: Option<Arc<ArxivMetadataIndex>>,
}

fn latex_partition(
    partition: ScanPartition,
    projection: ResolvedProjection,
    options: ReadOptions,
    index: Option<Arc<ArxivMetadataIndex>>,
) -> ScanPartition {
    scan_partition(async move {
        let reader = partition.await?;
        let ordering = reader.capability().ordering;
        let output = RecordMetadata {
            schema: projection.schema.clone(),
            source: reader.metadata().source.clone(),
        };
        let state = LatexScanState {
            reader,
            pending: Vec::new().into_iter(),
            projection,
            options,
            index,
        };
        let batches = stream::try_unfold(state, next_latex_batch);
        Ok(StreamReader::records(batches, output, ordering))
    })
}

async fn next_latex_batch(
    mut state: LatexScanState,
) -> Result<Option<(RecordBatch, LatexScanState)>> {
    loop {
        if let Some(batch) = state.pending.next() {
            let batch = if let Some(index) = &state.index {
                index.attach_articles(batch)?
            } else {
                batch
            };
            let batch = project_batch(
                batch,
                state.projection.indices.as_deref(),
                state.projection.schema.clone(),
            )?;
            return Ok(Some((batch, state)));
        }
        let Some(batch) = state.reader.next().await? else {
            return Ok(None);
        };
        state.pending = expand(&batch, &state.options).await?.into_iter();
    }
}

async fn expand(batch: &RecordBatch, options: &ReadOptions) -> Result<Vec<RecordBatch>> {
    let name = batch["entry_name"].as_binary::<i64>().value(0);
    let path =
        std::str::from_utf8(name).map_err(|_| error::decode("source article name is not UTF-8"))?;
    let mut stem = path.to_owned();
    for suffix in [".gz", ".tar", ".tex"] {
        if let Some(value) = stem.strip_suffix(suffix) {
            stem = value.to_owned();
        }
    }
    let article_id = id::pdf_id(format!("{stem}.pdf").as_bytes())?;
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
    let packaging = if is_tar(&data) {
        ArxivSourcePackaging::Tar
    } else {
        let text = std::str::from_utf8(&data)
            .map_err(|_| error::decode("source is neither a TAR nor UTF-8 LaTeX"))?;
        if !["\\documentclass", "\\documentstyle", "\\begin{document}"]
            .iter()
            .any(|marker| text.contains(marker))
        {
            return Err(error::decode(
                "single-file source is not recognized as LaTeX",
            ));
        }
        ArxivSourcePackaging::Single {
            name: b"main.tex".to_vec(),
        }
    };
    let input = bytes_reader(
        Bytes::from(data),
        Some(ObjectRef::new(format!("{shard}#entry={outer_index}"))),
    );
    let mut reader = open_source_package(
        Box::new(input),
        &article_id,
        packaging,
        ArxivSourceOptions::default(),
    )
    .await?;
    let article = LatexArticle {
        id: &article_id,
        shard,
        outer_index,
        name,
    };
    let mut output = LatexBatches {
        builder: RecordBatchBuilder::new(latex_schema(), None)?,
        batches: Vec::new(),
        text_bytes: 0,
        max_rows: options.batch_size.get(),
    };
    while let Some(batch) = reader.next().await? {
        for row in 0..batch.num_rows() {
            output.append_member(&article, &batch, row)?;
        }
    }
    output.finish()
}

struct LatexArticle<'a> {
    id: &'a str,
    shard: &'a str,
    outer_index: u64,
    name: &'a [u8],
}

struct LatexBatches {
    builder: RecordBatchBuilder,
    batches: Vec<RecordBatch>,
    text_bytes: usize,
    max_rows: usize,
}

impl LatexBatches {
    fn append_member(
        &mut self,
        article: &LatexArticle<'_>,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<()> {
        let source_name = batch["entry_name"].as_binary::<i64>().value(row);
        if !is_tex(source_name)? {
            return Ok(());
        }
        let text = std::str::from_utf8(batch["data"].as_binary::<i64>().value(row))
            .map_err(|_| error::decode("LaTeX member is not UTF-8"))?;
        let source_index = batch["entry_index"]
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(row);
        self.builder.push(|output| {
            output.append("article_id", article.id)?;
            output.append("format", "latex")?;
            output.append("shard_uri", article.shard)?;
            output.append("entry_index", article.outer_index)?;
            output.append("entry_name", article.name)?;
            output.append("source_index", source_index)?;
            output.append("source_name", source_name)?;
            output.append("text", text)
        })?;
        self.text_bytes += text.len();
        if self.builder.len() >= self.max_rows || self.text_bytes >= 4 * 1024 * 1024 {
            let next = RecordBatchBuilder::new(latex_schema(), None)?;
            let batch = std::mem::replace(&mut self.builder, next).finish()?;
            self.batches.push(batch);
            self.text_bytes = 0;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<RecordBatch>> {
        if self.batches.is_empty() && self.builder.is_empty() {
            return Err(error::decode(
                "source package contains no non-ancillary LaTeX files",
            ));
        }
        if !self.builder.is_empty() {
            self.batches.push(self.builder.finish()?);
        }
        Ok(self.batches)
    }
}

fn is_tex(name: &[u8]) -> Result<bool> {
    if !name.to_ascii_lowercase().ends_with(b".tex") {
        return Ok(false);
    }
    let name =
        std::str::from_utf8(name).map_err(|_| error::decode("LaTeX member name is not UTF-8"))?;
    let parts = name.split('/').collect::<Vec<_>>();
    if name.starts_with('/') || parts.contains(&"..") || name.contains('\\') {
        return Err(error::decode("unsafe source member name"));
    }
    Ok(!parts.iter().any(|part| part.eq_ignore_ascii_case("anc"))
        && name.to_ascii_lowercase().ends_with(".tex"))
}
fn is_tar(data: &[u8]) -> bool {
    if data.len() < 512 {
        return false;
    }
    if &data[257..262] == b"ustar" {
        return true;
    }
    let checksum = std::str::from_utf8(&data[148..156])
        .ok()
        .and_then(|s| u64::from_str_radix(s.trim_matches(['\0', ' ']), 8).ok());
    let sum: u64 = data[..512]
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            if (148..156).contains(&i) {
                32
            } else {
                u64::from(v)
            }
        })
        .sum();
    checksum == Some(sum)
}

#[cfg(test)]
mod tests;
