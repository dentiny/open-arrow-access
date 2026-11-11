use super::{JsonFormat, JsonInferenceOptions};
use crate::FileAccessor;
use open_arrow_access_core::SchemaRef;

/// Read JSON object streams through the shared file accessor. See [`JsonFormat`].
pub type JsonAccessor<B> = FileAccessor<B, JsonFormat>;

impl<B> FileAccessor<B, JsonFormat> {
    /// Infer a fixed schema from a bounded sample when opening each input.
    pub fn infer(inner: B) -> Self {
        Self::from_format(inner, JsonFormat::infer())
    }

    pub fn with_inference_options(self, options: JsonInferenceOptions) -> Self {
        self.map_format(|format| format.with_inference_options(options))
    }

    pub fn new(inner: B, schema: SchemaRef) -> Self {
        Self::from_format(inner, JsonFormat::new(schema))
    }
}
