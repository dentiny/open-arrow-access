use open_arrow_access_core::Result;

use super::inference::JsonInferenceOptions;
impl JsonInferenceOptions {
    pub(super) fn validate(self) -> Result<()> {
        crate::sample_limits::validate_sample_limits(self.max_records, self.max_bytes, "infer_json")
    }
}

pub(super) fn validate_record(value: &serde_json::Value) -> Result<()> {
    if !value.is_object() {
        return Err(super::error::non_object_inference_record());
    }
    Ok(())
}
