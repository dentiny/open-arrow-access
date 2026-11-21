use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, Field};
use futures::stream;

use crate::{
    ByteReader, OrderRequirement, ReadOptions, RecordBatch, RecordBatchBuilder, RecordMetadata,
    RecordReader, Result, Schema, SchemaRef, StreamReader,
    decompression::tar::{TarLimits, TarStream},
};

use super::{ArchiveFormat, ArchiveOptions, validation};

/// Fixed, provider-independent schema for selected archive members.
/// Names and contents are raw binary; `source_uri` is the caller's object locator.
pub fn archive_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("source_uri", DataType::LargeUtf8, false),
                Field::new("entry_index", DataType::UInt64, false),
                Field::new("entry_name", DataType::LargeBinary, false),
                Field::new("data", DataType::LargeBinary, false),
            ]))
        })
        .clone()
}

/// Assemble a pull-based reader without polling input or buffering any member.
pub(super) fn open_reader(
    input: Box<dyn ByteReader>,
    metadata: RecordMetadata,
    read: ReadOptions,
    options: Arc<ArchiveOptions>,
) -> Result<Box<dyn RecordReader>> {
    let source = metadata.source.as_ref().unwrap().locator().to_owned();
    let batches: crate::RecordStream = match &options.format {
        ArchiveFormat::Single { name } => {
            let name = name.clone();
            Box::pin(stream::once(async move {
                let data = read_payload(input).await?;
                let mut builder = RecordBatchBuilder::new(archive_schema(), read.projection)?;
                append_member(&mut builder, &source, 0, &name, &data)?;
                builder.finish()
            }))
        }
        ArchiveFormat::Tar => {
            let tar = TarStream::new(
                input,
                TarLimits {
                    max_entries: usize::MAX,
                    max_entry_bytes: u64::MAX,
                },
            );
            Box::pin(stream::try_unfold(tar, move |mut tar| {
                let source = source.clone();
                let read = read.clone();
                let options = options.clone();
                async move {
                    let batch = read_tar_batch(&mut tar, &source, &read, &options).await?;
                    Ok(batch.map(|batch| (batch, tar)))
                }
            }))
        }
    };
    Ok(Box::new(StreamReader::records(
        batches,
        metadata,
        OrderRequirement::PreserveGlobal,
    )))
}

/// Filter from headers and fill only the current output batch.
async fn read_tar_batch(
    tar: &mut TarStream,
    source: &str,
    read: &ReadOptions,
    options: &ArchiveOptions,
) -> Result<Option<RecordBatch>> {
    let mut builder = RecordBatchBuilder::new(archive_schema(), read.projection.clone())?;
    let mut bytes = 0u64;
    while builder.len() < read.batch_size.get()
        && (builder.is_empty() || bytes < options.target_bytes.get() as u64)
    {
        let Some(member) = tar.next_entry().await? else {
            break;
        };
        if !member.entry.is_file || !(options.filter)(&member.entry)? {
            drop(member.reader);
            continue;
        }
        let data = read_payload(member.reader).await?;
        bytes = bytes.saturating_add(member.entry.size);
        append_member(
            &mut builder,
            source,
            member.entry.index as u64,
            &member.entry.name,
            &data,
        )?;
    }
    if builder.is_empty() {
        Ok(None)
    } else {
        Ok(Some(builder.finish()?))
    }
}

/// Consume through EOF and collect the complete original payload.
async fn read_payload(mut input: Box<dyn ByteReader>) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    while let Some(chunk) = input.next().await? {
        validation::validate_chunk(&chunk)?;
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

fn append_member(
    builder: &mut RecordBatchBuilder,
    source: &str,
    index: u64,
    name: &[u8],
    data: &[u8],
) -> Result<()> {
    builder.push(|row| {
        row.append("source_uri", source)?;
        row.append("entry_index", index)?;
        row.append("entry_name", name)?;
        row.append("data", data)
    })
}
