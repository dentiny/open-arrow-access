//! Stream a shard once and hand owned article payloads to the shared executor.
use super::{ArxivReadRequest, ArxivRecords, record_schema, validation};
use futures::{TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ObjectRef, OrderRequirement, RecordBatchBuilder,
    RecordMetadata, Result, ScanPartition, ScanPlan, StreamReader, read_all, scan_partition,
};
use open_arrow_access_decompression::tar::{TarEntry, TarStream};

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
                .open_sequential(
                    ByteReadRequest::new(shard.clone()).with_options(request.byte_options),
                )
                .await?;
            let archive = TarStream::new(input, service.tar_limits.clone());
            Ok(stream::try_unfold(archive, move |mut archive| {
                let shard = shard.clone();
                async move {
                    while let Some(member) = archive.next_entry().await? {
                        if !member.entry.is_file {
                            drop(member.reader);
                            continue;
                        }
                        // The executor polls discovery only while a resident slot
                        // is available. Read one article, not a full shard/index.
                        let data = read_all(member.reader, "read_arxiv_article").await?;
                        return Ok(Some((member_partition(shard, member.entry, data), archive)));
                    }
                    Ok(None)
                }
            }))
        })
        .try_flatten();
        Ok(ScanPlan::from_stream(metadata, discovery)
            .with_streaming_discovery()
            .with_ordering(OrderRequirement::Reorderable))
    }
}

fn member_partition(shard: String, entry: TarEntry, data: Vec<u8>) -> ScanPartition {
    scan_partition(async move {
        let output = RecordMetadata {
            schema: record_schema(),
            source: Some(ObjectRef::new(&shard)),
        };
        let batches = stream::once(async move {
            let mut builder = RecordBatchBuilder::new(record_schema(), None)?;
            builder.push(|row| {
                row.append("shard_uri", shard.as_str())?;
                row.append("entry_index", entry.index as u64)?;
                row.append("entry_name", entry.name.as_slice())?;
                row.append("data", data.as_slice())
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
