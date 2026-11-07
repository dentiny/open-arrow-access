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
    pub fn with_schema(mut self, schema: XmlSchema) -> Self {
        self.format = self.format.with_schema(schema);
        self
    }

    /// Bind record-path prefixes for schema inference.
    pub fn with_namespaces(mut self, bindings: &[NamespaceBinding<'_>]) -> Self {
        self.format = self.format.with_namespaces(bindings);
        self
    }

    /// Override inference sampling limits. Used when no schema is supplied.
    pub fn with_inference_options(mut self, options: InferenceOptions) -> Self {
        self.format = self.format.with_inference_options(options);
        self
    }

    /// Override XML parsing limits; invalid limits fail before input I/O.
    pub fn with_limits(mut self, limits: XmlLimits) -> Self {
        self.format = self.format.with_limits(limits);
        self
    }
}
