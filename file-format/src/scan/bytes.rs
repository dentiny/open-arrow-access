use futures::{Stream, TryStreamExt};
use open_arrow_access_core::{
    Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, bytes_reader,
};

/// Bind owned byte chunks to independent decoders without opening them during discovery.
pub(crate) fn into_plan<F>(
    chunks: impl Stream<Item = Result<Bytes>> + Send + 'static,
    metadata: RecordMetadata,
    format: F,
    request: ReadRequest,
) -> ScanPlan
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + Clone + 'static,
{
    let partitions = chunks.map_ok(move |bytes| {
        let format = format.clone();
        let request = request.clone();
        let partition: ScanPartition = Box::pin(async move {
            let input = bytes_reader(bytes, Some(request.object.clone()));
            format.open_input(Box::new(input), request).await
        });
        partition
    });
    ScanPlan::from_stream(metadata, partitions)
}
