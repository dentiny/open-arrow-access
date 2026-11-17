use std::future::Future;

use futures::{Stream, TryStreamExt, stream};
use open_arrow_access_core::{
    Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt, RecordReader, Result, ScanPartition,
    bytes_reader,
};

/// Format-specific framing of independently decodable byte chunks.
/// The row and byte targets retain each format's existing framing semantics.
pub(crate) trait ByteChunks: Send + 'static {
    fn next_chunk(
        &mut self,
        rows: usize,
        target_bytes: usize,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send;
}

/// Discover owned byte partitions lazily; decoder creation stays in the partition.
/// In particular, CSV initialization may inspect the source only on the first poll.
pub(crate) fn byte_chunk_partitions<C, F>(
    chunks: impl Future<Output = Result<C>> + Send + 'static,
    format: F,
    request: ReadRequest,
    target_bytes: usize,
) -> impl Stream<Item = Result<ScanPartition>> + Send
where
    C: ByteChunks,
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + Clone + 'static,
{
    stream::once(async move {
        let chunks = chunks.await?;
        let rows = request.options.batch_size.get();
        Ok(stream::try_unfold(chunks, move |mut chunks| {
            let format = format.clone();
            let request = request.clone();
            async move {
                let Some(bytes) = chunks.next_chunk(rows, target_bytes).await? else {
                    return Ok(None);
                };
                let partition: ScanPartition = Box::pin(async move {
                    let input = bytes_reader(bytes, Some(request.object.clone()));
                    format.open_input(Box::new(input), request).await
                });
                Ok(Some((partition, chunks)))
            }
        }))
    })
    .try_flatten()
}
