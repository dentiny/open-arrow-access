//! Stream a shard once and hand owned article payloads to the shared executor.
use super::{ArxivReadRequest, ArxivRecords, record_schema, validation};
use futures::stream;
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
        let byte_options = request.byte_options;
        let discovery = stream::try_unfold(None::<TarStream>, move |archive| {
            let service = service.clone();
            let shard = shard.clone();
            async move {
                let mut archive = match archive {
                    Some(archive) => archive,
                    None => TarStream::new(
                        service
                            .storage
                            .open_sequential(
                                ByteReadRequest::new(shard.clone()).with_options(byte_options),
                            )
                            .await?,
                        service.tar_limits.clone(),
                    ),
                };
                while let Some(member) = archive.next_entry().await? {
                    if !member.entry.is_file {
                        drop(member.reader);
                        continue;
                    }
                    let data = read_all(member.reader, "read_arxiv_article").await?;
                    return Ok(Some((
                        member_partition(shard, member.entry, data),
                        Some(archive),
                    )));
                }
                Ok(None)
            }
        });
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
        let mut builder = RecordBatchBuilder::new(record_schema(), None)?;
        builder.push(|row| {
            row.append("shard_uri", shard.as_str())?;
            row.append("entry_index", entry.index as u64)?;
            row.append("entry_name", entry.name.as_slice())?;
            row.append("data", data.as_slice())
        })?;
        let batches = stream::iter([builder.finish()]);
        Ok(StreamReader::records(
            batches,
            output,
            OrderRequirement::Reorderable,
        ))
    })
}

#[cfg(test)]
mod tests;
