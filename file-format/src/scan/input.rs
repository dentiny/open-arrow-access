use open_arrow_access_core::{
    Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt, RecordReader, ScanPartition, bytes_reader,
};

pub(crate) fn decode_partition<F>(format: F, bytes: Bytes, request: ReadRequest) -> ScanPartition
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    Box::pin(async move {
        let input = bytes_reader(bytes, Some(request.object.clone()));
        format.open_input(Box::new(input), request).await
    })
}
