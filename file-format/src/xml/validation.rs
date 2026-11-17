use open_arrow_access_core::Result;

pub(super) fn validate_inference(options: &xml_arrow::InferenceOptions) -> Result<()> {
    crate::sample_limits::validate_sample_limits(options.max_records, options.max_bytes, "read_xml")
}
