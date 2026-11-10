use futures::{
    Stream, TryStreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{Result, ScanPartition};

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

    pub(crate) fn into_partitions(
        self,
        rows: usize,
        target_bytes: usize,
        size: impl Fn(&T) -> usize,
        partition: impl FnMut(Vec<T>) -> ScanPartition,
    ) -> impl Stream<Item = Result<ScanPartition>> {
        stream::try_unfold((self, size), move |(mut chunks, size)| async move {
            Ok(chunks
                .next_chunk(rows, target_bytes, &size)
                .await?
                .map(|records| (records, (chunks, size))))
        })
        .map_ok(partition)
    }

    async fn next_chunk(
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
