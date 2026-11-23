use super::CsvFormat;

/// Header policy shared by explicit-schema and schema-inference readers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CsvHeader {
    /// Match the full explicit schema's names, or detect from a sample when inferring.
    /// Ambiguous inference samples are treated as data to preserve the first row.
    #[default]
    Auto,
    /// Consume the first record as a header.
    Present,
    /// Decode every record as data.
    Absent,
}

impl From<bool> for CsvHeader {
    fn from(has_header: bool) -> Self {
        if has_header {
            Self::Present
        } else {
            Self::Absent
        }
    }
}

/// Explicit structural CSV options shared by the framer and Arrow decoder.
#[derive(Clone, Copy, Debug)]
pub struct CsvDialect {
    /// Detect, consume, or retain the first record. Defaults to Auto.
    pub header: CsvHeader,
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
            header: CsvHeader::Auto,
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
            .with_header(self.header == CsvHeader::Present)
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
}

/// Inspect only suffixes; object locators are not URLs or filesystem paths here.
pub(super) fn delimiter_from_path(path: &str) -> Option<u8> {
    let (_, extension) = path.rsplit_once('.')?;
    if extension.eq_ignore_ascii_case("csv") {
        Some(b',')
    } else if extension.eq_ignore_ascii_case("tsv") {
        Some(b'\t')
    } else {
        None
    }
}
