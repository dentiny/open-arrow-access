//! Stream attachment-free LaTeX from single sources or article TAR packages.
use super::{ArxivContentFormat, ArxivSourceOptions, content::content_schema, error, validation};
use arrow_array::cast::AsArray;
use futures::{AsyncReadExt, StreamExt, io::Cursor, stream};
use open_arrow_access_core::{
    ByteMetadata, Bytes, ObjectRef, ReadOptions, RecordBatch, RecordBatchBuilder, RecordStream,
    Result, bytes_reader, into_async_read, read_all,
};
use open_arrow_access_decompression::{
    Codec, DecodedInput, decoded_reader,
    gzip::Gzip,
    tar::{TarLimits, TarStream},
};
use std::num::NonZeroUsize;

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
pub(super) async fn read(batch: RecordBatch, options: ReadOptions) -> Result<RecordStream> {
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

/// Recognize PDF, PostScript, and DVI documents by their leading bytes.
fn is_non_tex_document(data: &[u8]) -> bool {
    data.starts_with(b"%PDF-") || data.starts_with(b"%!PS") || data.starts_with(b"\xf7\x02")
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
    if is_non_tex_document(&data) {
        return Err(error::missing_latex(batch, article_id));
    }
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
            let output = read_latex_batch(&mut tar, &batch, &article_id, &options).await?;
            if output.num_rows() == 0 {
                if !emitted {
                    return Err(error::missing_latex(&batch, &article_id));
                }
                Ok(None)
            } else {
                Ok(Some((output, (tar, true))))
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
