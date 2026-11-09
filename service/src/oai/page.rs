use open_arrow_access_core::{ByteMetadata, ByteReader, Bytes, StreamReader};

/// One page. Deleted records and all provider metadata remain in `bytes`.
#[derive(Clone, Debug)]
pub struct OaiPage {
    pub bytes: Bytes,
    pub resumption_token: Option<String>,
    pub record_count: usize,
}
impl OaiPage {
    pub fn reader(&self) -> Box<dyn ByteReader> {
        Box::new(StreamReader::new(
            futures::stream::iter([Ok(self.bytes.clone())]),
            ByteMetadata {
                source: None,
                content_length: Some(self.bytes.len() as u64),
                version: None,
            },
        ))
    }
}
