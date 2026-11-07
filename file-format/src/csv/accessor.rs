use super::{CsvFileFormat, CsvFormat};
use crate::FileAccessor;
use open_arrow_access_core::SchemaRef;

/// Read CSV/TSV through the shared file accessor. See [`CsvFileFormat`].
pub type CsvAccessor<B> = FileAccessor<B, CsvFileFormat>;

impl<B> FileAccessor<B, CsvFileFormat> {
    pub fn new(inner: B, schema: SchemaRef) -> Self {
        Self::from_format(inner, CsvFileFormat::new(schema))
    }

    /// Configure delimiter, header handling and other Arrow CSV options.
    pub fn with_format(mut self, format: CsvFormat) -> Self {
        self.format = self.format.with_format(format);
        self
    }
}
