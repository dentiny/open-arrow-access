//! Member partitions for one indexed TAR; the shared crawl executor bounds I/O and parsing.
use super::{ArxivReadRequest, ArxivRecords, ArxivService, record_schema, validation};
use futures::{TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ObjectRef, OrderRequirement, RecordBatchBuilder,
    RecordMetadata, Result, ScanPartition, ScanPlan, StreamReader, scan_partition,
};
use open_arrow_access_decompression::tar::{TarArchive, TarEntry};
use std::{ops::Range, sync::Arc};

impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static> ArxivRecords<A> {
    pub(super) fn plan_shard_entries(
        &self,
        shard: String,
        request: ArxivReadRequest,
    ) -> Result<ScanPlan> {
        validation::validate_shard(&shard, request.month)?;
        let metadata = RecordMetadata {
            schema: record_schema(),
            source: Some(ObjectRef::new(&shard)),
        };
        let service = self.service.clone();
        let discovery = stream::once(async move {
            let input = service
                .storage
                .open(ByteReadRequest::new(shard.clone()).with_options(request.byte_options))
                .await?;
            let archive = TarArchive::open(input, service.tar_limits.clone()).await?;
            let length = archive.content_length();
            let partitions = archive
                .entries()
                .filter(|entry| entry.is_file)
                .map(|entry| {
                    let range = archive.entry_range(entry.index)?;
                    Ok(member_partition(
                        service.clone(),
                        shard.clone(),
                        entry.clone(),
                        range,
                        length,
                        request.clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(stream::iter(partitions.into_iter().map(Ok)))
        })
        .try_flatten();
        Ok(ScanPlan::from_stream(metadata, discovery).with_ordering(OrderRequirement::Reorderable))
    }
}

fn member_partition<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static>(
    service: Arc<ArxivService<A>>,
    shard: String,
    entry: TarEntry,
    range: Range<u64>,
    length: u64,
    request: ArxivReadRequest,
) -> ScanPartition {
    scan_partition(async move {
        let output = RecordMetadata {
            schema: record_schema(),
            source: Some(ObjectRef::new(&shard)),
        };
        let batches = stream::once(async move {
            // Each task owns its reader: no shared seek mutex around network I/O.
            let mut input = service
                .storage
                .open(
                    ByteReadRequest::new(shard.clone())
                        .with_options(request.byte_options)
                        .with_content_length(length),
                )
                .await?;
            let data = input.read_range(range).await?;
            let mut builder = RecordBatchBuilder::new(record_schema(), None)?;
            builder.push(|row| {
                row.append("shard_uri", shard.as_str())?;
                row.append("entry_index", entry.index as u64)?;
                row.append("entry_name", entry.name.as_slice())?;
                row.append("data", data.as_ref())
            })?;
            builder.finish()
        });
        Ok(StreamReader::records(
            batches,
            output,
            OrderRequirement::Reorderable,
        ))
    })
}

#[cfg(test)]
mod tests;
