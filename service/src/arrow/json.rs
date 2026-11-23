//! Explicit JSON metadata-to-Arrow binding; never follows links or continuations.
use super::RecordPage;
use crate::common::ApiDocument;
use open_arrow_access_core::{ReadRequest, ReaderFactory, RecordReader, Result};
use serde_json::Value;

impl ApiDocument {
    /// Bind a JSON pointer's value to an explicit Arrow format/schema.
    /// Arrays become newline-separated records; other values become one record.
    /// Only this fetched page is decoded; links and future pages are not fetched.
    pub fn records_at<F>(&self, pointer: &str, format: F) -> Result<RecordPage<F>>
    where
        F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
    {
        let selected = self
            .value()
            .pointer(pointer)
            .ok_or_else(|| crate::common::error::missing_field(pointer))?;
        let mut bytes = Vec::new();
        match selected {
            Value::Array(records) => {
                for record in records {
                    serde_json::to_writer(&mut bytes, record)
                        .map_err(crate::common::error::serialize_metadata)?;
                    bytes.push(b'\n');
                }
            }
            record => serde_json::to_writer(&mut bytes, record)
                .map_err(crate::common::error::serialize_metadata)?,
        }
        Ok(RecordPage::new(bytes.into(), format))
    }
}
