use futures::{
    TryStreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{RecordMetadata, Result, ScanPartition, ScanPlan};

/// Batches complete records with one-record lookahead and a soft byte target.
/// The row limit is strict; an oversized first record is emitted on its own.
pub(crate) struct RecordChunks<T> {
    records: BoxStream<'static, Result<T>>,
    pending: Option<T>,
}

impl<T> RecordChunks<T> {
    pub(crate) fn new(records: BoxStream<'static, Result<T>>) -> Self {
        Self {
            records,
            pending: None,
        }
    }

    /// Discover bounded chunks lazily; conversion stays inside each partition future.
    pub(crate) fn into_plan(
        self,
        metadata: RecordMetadata,
        rows: usize,
        target_bytes: usize,
        size: impl Fn(&T) -> usize + Send + Sync + 'static,
        partition: impl Fn(Vec<T>, RecordMetadata) -> ScanPartition + Send + 'static,
    ) -> ScanPlan
    where
        T: Send + 'static,
    {
        let output = metadata.clone();
        let partitions = stream::try_unfold(
            (self, size, partition),
            move |(mut chunks, size, partition)| {
                let metadata = output.clone();
                async move {
                    let Some(records) = chunks.next_chunk(rows, target_bytes, &size).await? else {
                        return Ok(None);
                    };
                    Ok(Some((
                        partition(records, metadata),
                        (chunks, size, partition),
                    )))
                }
            },
        );
        ScanPlan::from_stream(metadata, partitions)
    }

    pub(crate) async fn next_chunk(
        &mut self,
        rows: usize,
        target_bytes: usize,
        size: impl Fn(&T) -> usize,
    ) -> Result<Option<Vec<T>>> {
        let mut chunk = Vec::new();
        let mut bytes = 0usize;
        while chunk.len() < rows {
            let record = match self.pending.take() {
                Some(record) => record,
                None => match self.records.try_next().await? {
                    Some(record) => record,
                    None => break,
                },
            };
            let size = size(&record);
            if !chunk.is_empty() && bytes.saturating_add(size) > target_bytes {
                self.pending = Some(record);
                break;
            }
            bytes = bytes.saturating_add(size);
            chunk.push(record);
        }
        Ok((!chunk.is_empty()).then_some(chunk))
    }
}
