use super::{CsvDialect, CsvFileFormat, CsvFormat, CsvInferenceOptions};
use crate::FileAccessor;
use open_arrow_access_core::SchemaRef;

/// Read CSV/TSV through the shared file accessor. See [`CsvFileFormat`].
pub type CsvAccessor<B> = FileAccessor<B, CsvFileFormat>;

impl<B> FileAccessor<B, CsvFileFormat> {
    /// Infer column types and detect headers from a bounded sample at open.
    pub fn infer(inner: B) -> Self {
        Self::from_format(inner, CsvFileFormat::infer())
    }

    /// Read nullable string columns named by the first CSV record. See
    /// [`CsvFileFormat::from_header`] for dialect and empty-input behavior.
    pub fn from_header(inner: B) -> Self {
        Self::from_format(inner, CsvFileFormat::from_header())
    }

    /// Read all columns as nullable strings, retaining header/dialect settings.
    pub fn all_varchar(self) -> Self {
        self.map_format(CsvFileFormat::all_varchar)
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
    pub fn with_dialect(self, dialect: CsvDialect) -> Self {
        self.map_format(|factory| factory.with_dialect(dialect))
    }
}
