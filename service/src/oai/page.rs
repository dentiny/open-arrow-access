use open_arrow_access_core::{ByteReader, Bytes, bytes_reader};

/// One page. Deleted records and all provider metadata remain in `bytes`.
#[derive(Clone, Debug)]
pub struct OaiPage {
    pub bytes: Bytes,
    pub resumption_token: Option<String>,
    pub record_count: usize,
}
impl OaiPage {
    pub fn reader(&self) -> Box<dyn ByteReader> {
        Box::new(bytes_reader(self.bytes.clone(), None))
    }
}
