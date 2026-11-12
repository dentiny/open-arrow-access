use open_arrow_access_core::Result;

pub(super) fn validate_inference(options: &xml_arrow::InferenceOptions) -> Result<()> {
    if options.max_records == 0 || options.max_bytes == 0 {
        return Err(super::error::invalid_inference_limits());
    }
    Ok(())
}
