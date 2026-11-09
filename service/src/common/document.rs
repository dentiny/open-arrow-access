use futures::stream;
use open_arrow_access_core::{ByteMetadata, ByteReader, Bytes, StreamReader};
use serde_json::Value;

/// Raw upstream response and parsed fields; unknown fields and exact raw bytes survive.
#[derive(Clone, Debug)]
pub struct ApiDocument {
    bytes: Bytes,
    value: Value,
}
impl ApiDocument {
    /// Keep exact upstream bytes and decode a JSON view without normalizing the raw response.
    pub fn from_bytes(bytes: Bytes) -> open_arrow_access_core::Result<Self> {
        let value = super::json_util::parse(&bytes)?;
        Ok(Self { bytes, value })
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn reader(&self) -> Box<dyn ByteReader> {
        let bytes = self.bytes.clone();
        let length = bytes.len() as u64;
        Box::new(StreamReader::new(
            stream::iter((!bytes.is_empty()).then_some(Ok(bytes))),
            ByteMetadata {
                source: None,
                content_length: Some(length),
                version: None,
            },
        ))
    }
}
