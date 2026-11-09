use open_arrow_access_core::Bytes;
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

    pub fn value(&self) -> &Value {
        &self.value
    }
}
