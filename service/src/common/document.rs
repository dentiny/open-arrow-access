use super::RecordPage;
use open_arrow_access_core::Bytes;
use open_arrow_access_core::{ReadRequest, ReaderFactory, RecordReader, Result};
use serde_json::Value;

/// Parsed API metadata, including unrecognized upstream fields.
#[derive(Clone, Debug)]
pub struct ApiDocument {
    value: Value,
}
impl ApiDocument {
    /// Parse JSON metadata; input bytes are released after decoding.
    pub fn from_bytes(bytes: Bytes) -> open_arrow_access_core::Result<Self> {
        let value = super::json_util::parse(&bytes)?;
        Ok(Self { value })
    }

    /// Apply provider checks and return the original document on success.
    /// Unknown fields are retained; validation errors pass through unchanged.
    pub fn validated(
        self,
        validate: impl FnOnce(&Value) -> open_arrow_access_core::Result<()>,
    ) -> open_arrow_access_core::Result<Self> {
        validate(&self.value)?;
        Ok(self)
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl ApiDocument {
    /// Bind a JSON pointer's value to an explicit Arrow format/schema.
    /// Arrays become newline-separated records; other values become one record.
    /// Only this fetched page is decoded; links and future pages are not fetched.
    pub fn records_at<F>(&self, pointer: &str, format: F) -> Result<RecordPage<F>>
    where
        F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
    {
        let selected = self
            .value
            .pointer(pointer)
            .ok_or_else(|| super::error::missing_field(pointer))?;
        let mut bytes = Vec::new();
        match selected {
            Value::Array(records) => {
                for record in records {
                    serde_json::to_writer(&mut bytes, record)
                        .map_err(super::error::serialize_metadata)?;
                    bytes.push(b'\n');
                }
            }
            record => serde_json::to_writer(&mut bytes, record)
                .map_err(super::error::serialize_metadata)?,
        }
        Ok(RecordPage::new(bytes.into(), format))
    }
}
