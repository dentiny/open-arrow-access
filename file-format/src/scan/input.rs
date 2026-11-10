use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Result, StreamReader,
    async_trait,
};

/// An owned, already framed unit. Each open has independent sequential state.
pub(crate) struct ChunkInput(pub(crate) Bytes);

#[async_trait]
impl Accessor for ChunkInput {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let metadata = ByteMetadata {
            source: Some(request.object),
            content_length: Some(self.0.len() as u64),
            version: None,
        };
        let bytes = self.0.clone();
        let chunks = stream::iter((!bytes.is_empty()).then_some(Ok(bytes)));
        Ok(Box::new(StreamReader::new(chunks, metadata)))
    }
}
