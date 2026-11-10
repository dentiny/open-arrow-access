use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, Bytes, Capability, Result, async_trait, bytes_reader,
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
        Ok(Box::new(bytes_reader(self.0.clone(), Some(request.object))))
    }
}
