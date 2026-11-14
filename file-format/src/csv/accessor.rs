use super::{CsvDialect, CsvFileFormat, CsvFormat, CsvHeader, CsvInferenceOptions};
use crate::FileAccessor;
use open_arrow_access_core::SchemaRef;

/// Read CSV/TSV through the shared file accessor. See [`CsvFileFormat`].
pub type CsvAccessor<B> = FileAccessor<B, CsvFileFormat>;

impl<B> FileAccessor<B, CsvFileFormat> {
    /// Infer column types and detect headers from a bounded sample at open.
    pub fn infer(inner: B) -> Self {
        Self::from_format(inner, CsvFileFormat::infer())
    }

    pub fn with_inference_options(self, options: CsvInferenceOptions) -> Self {
        self.map_format(|format| format.with_inference_options(options))
    }

    pub fn new(inner: B, schema: SchemaRef) -> Self {
        Self::from_format(inner, CsvFileFormat::new(schema))
    }

    /// Configure delimiter, header handling and other Arrow CSV options.
    pub fn with_format(self, format: CsvFormat) -> Self {
        self.map_format(|factory| factory.with_format(format))
    }
    /// Set header handling independently of schema sampling. Defaults to Auto.
    pub fn with_header(self, header: CsvHeader) -> Self {
        self.map_format(|format| format.with_header(header))
    }

    pub fn with_dialect(self, dialect: CsvDialect) -> Self {
        self.map_format(|factory| factory.with_dialect(dialect))
    }
}
