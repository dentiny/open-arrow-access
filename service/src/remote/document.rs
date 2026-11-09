use futures::stream;
use open_arrow_access_core::{ByteMetadata, ByteReader, Bytes, StreamReader};
use serde_json::Value;

/// Raw upstream response and parsed fields; unknown fields and exact raw bytes survive.
#[derive(Clone, Debug)]
pub struct ApiDocument {
    pub(super) bytes: Bytes,
    pub(super) value: Value,
}
impl ApiDocument {
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
