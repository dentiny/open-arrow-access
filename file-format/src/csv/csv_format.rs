use std::io::Read;

use arrow_schema::{ArrowError, Schema};
use regex::Regex;

/// Arrow CSV options with header validation retained on Arrow 58.
#[derive(Clone, Debug, Default)]
pub struct CsvFormat {
    pub(super) inner: arrow_csv::reader::Format,
    pub(super) has_header: bool,
    pub(super) validate_header: bool,
}

impl CsvFormat {
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.inner = self.inner.with_header(has_header);
        self.has_header = has_header;
        self
    }

    /// Validate the first record against the full schema when headers are enabled.
    pub fn with_header_validation(mut self, validate_header: bool) -> Self {
        self.validate_header = validate_header;
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.inner = self.inner.with_delimiter(delimiter);
        self
    }

    pub fn with_escape(mut self, escape: u8) -> Self {
        self.inner = self.inner.with_escape(escape);
        self
    }

    pub fn with_quote(mut self, quote: u8) -> Self {
        self.inner = self.inner.with_quote(quote);
        self
    }

    pub fn with_terminator(mut self, terminator: u8) -> Self {
        self.inner = self.inner.with_terminator(terminator);
        self
    }

    pub fn with_comment(mut self, comment: u8) -> Self {
        self.inner = self.inner.with_comment(comment);
        self
    }

    pub fn with_null_regex(mut self, null_regex: Regex) -> Self {
        self.inner = self.inner.with_null_regex(null_regex);
        self
    }

    pub fn with_truncated_rows(mut self, allow: bool) -> Self {
        self.inner = self.inner.with_truncated_rows(allow);
        self
    }

    pub fn infer_schema<R: Read>(
        &self,
        reader: R,
        max_records: Option<usize>,
    ) -> Result<(Schema, usize), ArrowError> {
        self.inner.infer_schema(reader, max_records)
    }
}
