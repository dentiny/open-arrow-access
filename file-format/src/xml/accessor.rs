use super::{XmlFormat, XmlLimits};
use crate::FileAccessor;
use xml_arrow::{InferenceOptions, NamespaceBinding, XmlSchema};

/// Read XML through the shared file accessor. See [`XmlFormat`].
pub type XmlAccessor<B> = FileAccessor<B, XmlFormat>;

impl<B> FileAccessor<B, XmlFormat> {
    /// See [`XmlFormat::new`].
    pub fn new(inner: B, record_path: impl Into<String>) -> Self {
        Self::from_format(inner, XmlFormat::new(record_path))
    }

    /// See [`XmlFormat::with_schema`].
    pub fn with_schema(self, schema: XmlSchema) -> Self {
        self.map_format(|factory| factory.with_schema(schema))
    }

    /// See [`XmlFormat::with_namespaces`].
    pub fn with_namespaces(self, bindings: &[NamespaceBinding<'_>]) -> Self {
        self.map_format(|factory| factory.with_namespaces(bindings))
    }

    /// See [`XmlFormat::with_inference_options`].
    pub fn with_inference_options(self, options: InferenceOptions) -> Self {
        self.map_format(|factory| factory.with_inference_options(options))
    }

    /// See [`XmlFormat::with_limits`].
    pub fn with_limits(self, limits: XmlLimits) -> Self {
        self.map_format(|factory| factory.with_limits(limits))
    }
}
