//! Opt-in source package expansion; filenames remain metadata, never disk paths.
use super::{ArxivArticle, ArxivArticleFormat, article_schema, error};
use futures::stream;
use open_arrow_access_core::{
    ByteReader, Bytes, ObjectRef, OrderRequirement, ReadOptions, RecordBatchBuilder,
    RecordMetadata, RecordReader, Result, StreamReader, bytes_reader, into_async_read, read_all,
};
use open_arrow_access_decompression::{
    Codec, decoded_reader,
    gzip::Gzip,
    tar::{TarLimits, TarStream},
};
use std::num::NonZeroUsize;

/// Packaging is explicit: a gzip file is not necessarily a TAR.
#[derive(Clone, Debug)]
pub enum ArxivSourcePackaging {
    Tar,
    GzipTar,
    /// A lone TeX/PDF/PostScript/etc. file, with its caller-supplied original name.
    Single {
        name: Vec<u8>,
    },
    GzipSingle {
        name: Vec<u8>,
    },
}

#[derive(Clone, Debug)]
pub struct ArxivSourceOptions {
    pub options: ReadOptions,
    pub tar_limits: TarLimits,
    /// Hard limit for the decoded package, including TAR headers/padding.
    /// The package is buffered up to this limit before rows are emitted.
    pub max_decoded_bytes: NonZeroUsize,
    /// Soft per-batch payload target. A complete member can exceed it.
    pub target_bytes: NonZeroUsize,
}
impl Default for ArxivSourceOptions {
    fn default() -> Self {
        Self {
            options: ReadOptions::default(),
            tar_limits: TarLimits::default(),
            max_decoded_bytes: NonZeroUsize::new(256 * 1024 * 1024).unwrap(),
            target_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
        }
    }
}

/// Expand one article payload from a website download or an outer bulk TAR member.
///
/// Returns format-neutral artifact rows with `format = "source_member"`, preserving
/// every regular member, including figures and `anc/` files, without an allowlist.
/// Directories are skipped. Duplicate/non-UTF-8 names retain physical indices.
/// Single-file sources require an explicit name; gzip CRC/truncation/trailing data
/// are checked through EOF before emitting rows. Inner archives are not expanded
/// recursively, LaTeX is not compiled, and external links are not fetched.
///
/// `source_uri` is inherited from the byte reader; retain the bulk member identity
/// in that locator when expanding an article from a bulk shard. Byte storage and
/// immutable source/version identity remain caller-owned.
pub async fn open_source_package(
    input: Box<dyn ByteReader>,
    article_id: impl Into<String>,
    packaging: ArxivSourcePackaging,
    options: ArxivSourceOptions,
) -> Result<Box<dyn RecordReader>> {
    let article_id = article_id.into();
    ArxivArticle::new(&article_id, ArxivArticleFormat::Latex).locator()?;
    let schema = options
        .options
        .resolve_projection(&article_schema())?
        .schema;
    let mut metadata = input.metadata().clone();
    let source_uri = metadata
        .source
        .as_ref()
        .map(ObjectRef::locator)
        .unwrap_or("")
        .to_owned();
    let input = if matches!(
        packaging,
        ArxivSourcePackaging::GzipTar | ArxivSourcePackaging::GzipSingle { .. }
    ) {
        metadata.content_length = None;
        let decoder = Gzip.decoder(Box::pin(into_async_read(input, "read_arxiv_source")));
        decoded_reader(
            decoder,
            metadata.clone(),
            NonZeroUsize::new(64 * 1024).unwrap(),
            "decode_arxiv_source",
            "gzip",
        )
    } else {
        input
    };
    let data = Bytes::from(read_bounded(input, options.max_decoded_bytes.get()).await?);
    let output = RecordMetadata {
        schema,
        source: metadata.source.clone(),
    };
    let source = metadata.source;
    match packaging {
        ArxivSourcePackaging::Single { name } | ArxivSourcePackaging::GzipSingle { name } => {
            if data.len() as u64 > options.tar_limits.max_entry_bytes {
                return Err(super::error::decode(
                    "source member exceeds max_entry_bytes",
                ));
            }
            let batches = stream::once(async move {
                let mut builder =
                    RecordBatchBuilder::new(article_schema(), options.options.projection)?;
                builder.push(|row| {
                    row.append("article_id", article_id.as_str())?;
                    row.append("format", "source_member")?;
                    row.append("source_uri", source_uri.as_str())?;
                    row.append("entry_index", 0_u64)?;
                    row.append("entry_name", name.as_slice())?;
                    row.append("data", data.as_ref())
                })?;
                builder.finish()
            });
            Ok(Box::new(StreamReader::records(
                batches,
                output,
                OrderRequirement::PreserveGlobal,
            )))
        }
        ArxivSourcePackaging::Tar | ArxivSourcePackaging::GzipTar => {
            let tar = TarStream::new(Box::new(bytes_reader(data, source)), options.tar_limits);
            let batches = stream::try_unfold(tar, move |mut tar| {
                let projection = options.options.projection.clone();
                let rows = options.options.batch_size.get();
                let target = options.target_bytes.get();
                let article_id = article_id.clone();
                let source_uri = source_uri.clone();
                async move {
                    let mut builder = RecordBatchBuilder::new(article_schema(), projection)?;
                    let mut bytes = 0_usize;
                    while builder.len() < rows && (builder.is_empty() || bytes < target) {
                        let Some(member) = tar.next_entry().await? else {
                            break;
                        };
                        if !member.entry.is_file {
                            drop(member.reader);
                            continue;
                        }
                        let data = read_all(member.reader, "read_arxiv_source_member").await?;
                        bytes = bytes.saturating_add(data.len());
                        builder.push(|row| {
                            row.append("article_id", article_id.as_str())?;
                            row.append("format", "source_member")?;
                            row.append("source_uri", source_uri.as_str())?;
                            row.append("entry_index", member.entry.index as u64)?;
                            row.append("entry_name", member.entry.name.as_slice())?;
                            row.append("data", data.as_slice())
                        })?;
                    }
                    if builder.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some((builder.finish()?, tar)))
                    }
                }
            });
            Ok(Box::new(StreamReader::records(
                batches,
                output,
                OrderRequirement::PreserveGlobal,
            )))
        }
    }
}

async fn read_bounded(mut input: Box<dyn ByteReader>, max_bytes: usize) -> Result<Vec<u8>> {
    let object = input.metadata().source.clone();
    let too_large =
        || error::decode("arXiv payload exceeds configured max_bytes").with_object(object.as_ref());
    if input
        .metadata()
        .content_length
        .is_some_and(|n| n > max_bytes as u64)
    {
        return Err(too_large());
    }
    let mut output = Vec::new();
    while let Some(chunk) = input.next().await? {
        if chunk.is_empty() {
            return Err(error::decode("empty arXiv byte chunk"));
        }
        if chunk.len() > max_bytes - output.len() {
            return Err(too_large());
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}
