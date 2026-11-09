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
