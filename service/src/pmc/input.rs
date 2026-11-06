use super::{capability, decode};
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, ObjectRef, Reader, Result, async_trait,
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
    let too_large = || {
        decode(format!(
            "{path}: exceeds configured {limit}-byte object limit"
        ))
    };
    if reader
        .metadata()
        .content_length
        .is_some_and(|size| size > limit as u64)
    {
        return Err(too_large());
    }
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        if bytes.is_empty() {
            return Err(Error::new(
                ErrorKind::Io,
                "read_pmc",
                format!("{path}: empty byte chunk"),
            ));
        }
        if bytes.len() > limit - output.len() {
            return Err(too_large());
        }
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

/// Normalized metadata JSONL for one batch, consumed by the existing JsonAccessor.
/// It never contains the PDF payload or the verbatim raw JSON column.
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
