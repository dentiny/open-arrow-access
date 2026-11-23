//! OAI envelope-to-Arrow binding, separate from protocol continuation.
use super::RecordPage;
use crate::oai::OaiPage;
use open_arrow_access_core::{ReadRequest, ReaderFactory, RecordReader};

impl OaiPage {
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
