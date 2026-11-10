use crate::common::RecordPage;
use open_arrow_access_core::{
    ByteReader, Bytes, ReadRequest, ReaderFactory, RecordReader, bytes_reader,
};

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

    /// Bind an explicit XML record path/schema to this already fetched page.
    /// Page bytes, deleted records and namespace context remain intact. The
    /// resumption token does not become an independent parallel fetch partition.
    pub fn records<F>(&self, format: F) -> RecordPage<F>
    where
        F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
    {
        RecordPage::new(self.bytes.clone(), format)
    }
}
