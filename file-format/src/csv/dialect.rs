use super::CsvFormat;

/// Explicit structural CSV options shared by the framer and Arrow decoder.
#[derive(Clone, Copy, Debug)]
pub struct CsvDialect {
    /// Consume and optionally validate the first record as a header.
    pub has_header: bool,
    pub validate_header: bool,
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    /// None accepts the standard CR/LF/CRLF terminators.
    pub terminator: Option<u8>,
    pub comment: Option<u8>,
    pub allow_truncated_rows: bool,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            has_header: false,
            validate_header: false,
            delimiter: b',',
            quote: b'"',
            escape: None,
            terminator: None,
            comment: None,
            allow_truncated_rows: false,
        }
    }
}

impl CsvDialect {
    pub(super) fn format(self) -> CsvFormat {
        let mut format = CsvFormat::default()
            .with_header(self.has_header)
            .with_header_validation(self.validate_header)
            .with_delimiter(self.delimiter)
            .with_quote(self.quote)
            .with_truncated_rows(self.allow_truncated_rows);
        if let Some(escape) = self.escape {
            format = format.with_escape(escape);
        }
        if let Some(terminator) = self.terminator {
            format = format.with_terminator(terminator);
        }
        if let Some(comment) = self.comment {
            format = format.with_comment(comment);
        }
        format
    }

    pub(super) fn reader_builder(self) -> csv_async::AsyncReaderBuilder {
        let mut builder = csv_async::AsyncReaderBuilder::new();
        builder
            .has_headers(false)
            .flexible(true)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .escape(self.escape)
            .comment(self.comment);
        if let Some(terminator) = self.terminator {
            builder.terminator(csv_async::Terminator::Any(terminator));
        }
        builder
    }
}
