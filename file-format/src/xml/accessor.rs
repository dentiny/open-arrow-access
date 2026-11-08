use super::{XmlFormat, XmlLimits};
use crate::FileAccessor;
use xml_arrow::{InferenceOptions, NamespaceBinding, XmlSchema};

/// Read XML through the shared file accessor. See [`XmlFormat`].
pub type XmlAccessor<B> = FileAccessor<B, XmlFormat>;

impl<B> FileAccessor<B, XmlFormat> {
    /// Select records and infer a schema from a bounded sample when opening.
    pub fn new(inner: B, record_path: impl Into<String>) -> Self {
        Self::from_format(inner, XmlFormat::new(record_path))
    }

    /// Use explicit field mappings instead of inferring a schema.
    pub fn with_schema(self, schema: XmlSchema) -> Self {
        self.map_format(|factory| factory.with_schema(schema))
    }

    /// Bind record-path prefixes for schema inference.
    pub fn with_namespaces(self, bindings: &[NamespaceBinding<'_>]) -> Self {
        self.map_format(|factory| factory.with_namespaces(bindings))
    }

    /// Override inference sampling limits. Used when no schema is supplied.
    pub fn with_inference_options(self, options: InferenceOptions) -> Self {
        self.map_format(|factory| factory.with_inference_options(options))
    }

    /// Override XML parsing limits; invalid limits fail before input I/O.
    pub fn with_limits(self, limits: XmlLimits) -> Self {
        self.map_format(|factory| factory.with_limits(limits))
    }
}
