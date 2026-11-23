//! Parse source-dialect records into canonical CSV chunks for schema sampling
//! and independently decoded scan partitions. Neither caller owns CSV framing.

use futures::io::{AsyncReadExt, Cursor};
use open_arrow_access_core::{ByteReader, Bytes, Result, into_async_read};

use super::{CsvDialect, CsvHeader, error};

/// Owns source BOM handling, dialect parsing, header replay, and canonical quoting.
/// Consumers choose row/byte targets without handling partial CSV records.
pub(super) struct CsvFramer {
    records: csv_async::AsyncReader<Box<dyn futures::io::AsyncRead + Unpin + Send>>,
    record: csv_async::ByteRecord,
    header: Option<csv_async::ByteRecord>,
    // Validate a header-only input once even when there are no data records.
    emitted: bool,
}

impl CsvFramer {
    pub(super) async fn new(input: Box<dyn ByteReader>, dialect: CsvDialect) -> Result<Self> {
        let mut input = into_async_read(input, "frame_csv");
        // csv-async's parser does not recognize a BOM split across input chunks.
        // Normalize only the source prefix, stopping as soon as a BOM is ruled out.
        let mut prefix = Vec::new();
        while prefix.len() < 3 && b"\xef\xbb\xbf".starts_with(&prefix) {
            let mut byte = [0];
            if input.read(&mut byte).await.map_err(error::input_error)? == 0 {
                break;
            }
            prefix.push(byte[0]);
        }
        if prefix == b"\xef\xbb\xbf" {
            prefix.clear();
        }
        // A skipped blank line disables the parser's own source-start BOM removal.
        prefix.insert(0, dialect.terminator.unwrap_or(b'\n'));
        let mut records = reader_builder(dialect)
            .create_reader(Box::new(Cursor::new(prefix).chain(input))
                as Box<dyn futures::io::AsyncRead + Unpin + Send>);
        let header = if dialect.header == CsvHeader::Present {
            let mut header = csv_async::ByteRecord::new();
            records
                .read_byte_record(&mut header)
                .await
                .map_err(error::record_error)?
                .then_some(header)
        } else {
            None
        };
        Ok(Self {
            records,
            record: csv_async::ByteRecord::new(),
            header,
            emitted: false,
        })
    }
}

impl crate::scan::byte_chunks::ByteChunks for CsvFramer {
    async fn next_chunk(&mut self, row_target: usize, byte_target: usize) -> Result<Option<Bytes>> {
        let mut output = Vec::new();
        let mut rows = 0;
        {
            // Quoting every field preserves data BOMs and arbitrary field bytes.
            // Leave row-length and UTF-8 validation to the Arrow worker.
            let mut writer = csv::WriterBuilder::new()
                .flexible(true)
                .quote_style(csv::QuoteStyle::Always)
                .from_writer(&mut output);
            if let Some(header) = &self.header {
                writer
                    .write_record(header.iter())
                    .map_err(error::write_error)?;
            }
            while rows < row_target {
                if !self
                    .records
                    .read_byte_record(&mut self.record)
                    .await
                    .map_err(error::record_error)?
                {
                    break;
                };
                writer
                    .write_record(self.record.iter())
                    .map_err(error::write_error)?;
                writer.flush().map_err(error::input_error)?;
                rows += 1;
                if writer.get_ref().len() >= byte_target {
                    break;
                }
            }
            writer.flush().map_err(error::input_error)?;
        }
        if rows == 0 && (self.emitted || output.is_empty()) {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(output.into()))
    }
}

fn reader_builder(dialect: CsvDialect) -> csv_async::AsyncReaderBuilder {
    let mut builder = csv_async::AsyncReaderBuilder::new();
    builder
        .has_headers(false)
        .flexible(true)
        .delimiter(dialect.delimiter)
        .quote(dialect.quote)
        .escape(dialect.escape)
        .comment(dialect.comment);
    if let Some(terminator) = dialect.terminator {
        builder.terminator(csv_async::Terminator::Any(terminator));
    }
    builder
}
