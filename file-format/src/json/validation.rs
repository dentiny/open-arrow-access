use open_arrow_access_core::Result;

use super::inference::JsonInferenceOptions;
impl JsonInferenceOptions {
    pub(super) fn validate(self) -> Result<()> {
        if self.max_records == 0 || self.max_bytes == 0 {
            return Err(super::error::invalid_inference_limits());
        }
        Ok(())
    }
}

pub(super) fn validate_record(value: &serde_json::Value) -> Result<()> {
    if !value.is_object() {
        return Err(super::error::non_object_inference_record());
    }
    Ok(())
}
