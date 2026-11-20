//! Opt-in source package expansion; filenames remain metadata, never disk paths.
use super::{ArxivArticle, ArxivArticleFormat, article_schema};
use futures::stream;
use open_arrow_access_core::{
    ByteReader, Bytes, ObjectRef, OrderRequirement, ReadOptions, RecordBatch, RecordBatchBuilder,
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
    /// Soft per-batch payload target. A complete member can exceed it.
    pub target_bytes: NonZeroUsize,
}
impl Default for ArxivSourceOptions {
    fn default() -> Self {
        Self {
            options: ReadOptions::default(),
            tar_limits: TarLimits::default(),
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
    let data = Bytes::from(read_all(input, "read_arxiv_source").await?);
    let output = RecordMetadata {
        schema,
        source: metadata.source.clone(),
    };
    match packaging {
        ArxivSourcePackaging::Single { name } | ArxivSourcePackaging::GzipSingle { name } => {
            open_single_source(data, name, article_id, source_uri, output, options)
        }
        ArxivSourcePackaging::Tar | ArxivSourcePackaging::GzipTar => {
            open_tar_source(data, article_id, source_uri, output, options)
        }
    }
}

fn open_single_source(
    data: Bytes,
    name: Vec<u8>,
    article_id: String,
    source_uri: String,
    output: RecordMetadata,
    options: ArxivSourceOptions,
) -> Result<Box<dyn RecordReader>> {
    if data.len() as u64 > options.tar_limits.max_entry_bytes {
        return Err(super::error::decode(
            "source member exceeds max_entry_bytes",
        ));
    }
    let batches = stream::once(async move {
        let mut builder = RecordBatchBuilder::new(article_schema(), options.options.projection)?;
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

fn open_tar_source(
    data: Bytes,
    article_id: String,
    source_uri: String,
    output: RecordMetadata,
    options: ArxivSourceOptions,
) -> Result<Box<dyn RecordReader>> {
    let tar = TarStream::new(
        Box::new(bytes_reader(data, output.source.clone())),
        options.tar_limits,
    );
    let batches = stream::try_unfold(tar, move |tar| {
        read_source_batch(
            tar,
            article_id.clone(),
            source_uri.clone(),
            options.options.clone(),
            options.target_bytes.get(),
        )
    });
    Ok(Box::new(StreamReader::records(
        batches,
        output,
        OrderRequirement::PreserveGlobal,
    )))
}

async fn read_source_batch(
    mut tar: TarStream,
    article_id: String,
    source_uri: String,
    options: ReadOptions,
    target: usize,
) -> Result<Option<(RecordBatch, TarStream)>> {
    let rows = options.batch_size.get();
    let mut builder = RecordBatchBuilder::new(article_schema(), options.projection)?;
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
