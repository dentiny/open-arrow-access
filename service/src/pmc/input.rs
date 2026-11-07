use super::{capability, validation};
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability,
    ObjectRef, Reader, Result, async_trait,
};

pub(super) async fn read_bounded<A>(
    accessor: &A,
    object: ObjectRef,
    options: ByteReadOptions,
    limit: usize,
) -> Result<Vec<u8>>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    let path = object.locator().to_owned();
    let mut reader = accessor
        .open(ByteReadRequest::new(object).with_options(options))
        .await?;
    validation::validate_content_length(reader.metadata().content_length, limit, &path)?;
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        validation::validate_chunk(&bytes, limit - output.len(), limit, &path)?;
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

/// Mapped scalar metadata consumed by the existing JsonAccessor.
pub(super) struct JsonInput(pub Bytes);

struct JsonReader {
    bytes: Option<Bytes>,
    metadata: ByteMetadata,
}

#[async_trait]
impl Accessor for JsonInput {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        capability(true)
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(JsonReader {
            bytes: Some(self.0.clone()),
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.0.len() as u64),
                version: None,
            },
        }))
    }
}

#[async_trait]
impl Reader for JsonReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        capability(true)
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        Ok(self.bytes.take().filter(|bytes| !bytes.is_empty()))
    }
}
#[async_trait]
impl ByteReader for JsonReader {}
