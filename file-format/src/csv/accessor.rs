use super::{CsvDialect, CsvFileFormat, CsvFormat, CsvHeader, CsvInferenceOptions, CsvSchemaMode};
use crate::FileAccessor;
use open_arrow_access_core::SchemaRef;

/// Read CSV/TSV through the shared file accessor. See [`CsvFileFormat`].
pub type CsvAccessor<B> = FileAccessor<B, CsvFileFormat>;

impl<B> FileAccessor<B, CsvFileFormat> {
    /// Infer column types and detect headers from a bounded sample at open.
    pub fn infer(inner: B) -> Self {
        Self::from_format(inner, CsvFileFormat::infer())
    }

    /// Share registered compression suffixes with the byte decompression layer.
    pub fn with_compression_factory(
        self,
        factory: open_arrow_access_decompression::CompressionFactory,
    ) -> Self {
        self.map_format(|format| format.with_compression_factory(factory))
    }

    pub fn with_inference_options(self, options: CsvInferenceOptions) -> Self {
        self.map_format(|format| format.with_inference_options(options))
    }

    /// Discover a schema with inferred types or all nullable Utf8 columns.
    /// Replaces any explicit schema; header and dialect settings are preserved.
    pub fn with_schema_mode(self, mode: CsvSchemaMode) -> Self {
        self.map_format(|format| format.with_schema_mode(mode))
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

    /// Set the delimiter explicitly, disabling `.csv`/`.tsv` extension detection.
    pub fn with_delimiter(self, delimiter: u8) -> Self {
        self.map_format(|format| format.with_delimiter(delimiter))
    }
}
