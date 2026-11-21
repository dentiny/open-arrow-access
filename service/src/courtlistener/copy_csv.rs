//! Streaming PostgreSQL COPY CSV framing, retaining quoted-empty vs unquoted NULL.
use super::error::copy_error;
use open_arrow_access_core::{ByteReader, Bytes, Result};

/// PostgreSQL COPY quoting contract for a pinned export (not inferred from content).
#[derive(Clone, Copy, Debug)]
pub struct CourtListenerCopyDialect {
    pub quote: u8,
    pub escape: u8,
}
impl Default for CourtListenerCopyDialect {
    fn default() -> Self {
        Self {
            quote: b'"',
            escape: b'\\',
        }
    }
}

pub(super) struct CopyCsv {
    reader: Box<dyn ByteReader>,
    chunk: Bytes,
    offset: usize,
    skip_lf: bool,
    max_fields: usize,
    dialect: CourtListenerCopyDialect,
    pub object: String,
    pub ordinal: u64,
}
impl CopyCsv {
    pub fn new(
        reader: Box<dyn ByteReader>,
        object: String,
        max_fields: usize,
        dialect: CourtListenerCopyDialect,
    ) -> Self {
        Self {
            reader,
            chunk: Bytes::new(),
            offset: 0,
            skip_lf: false,
            max_fields,
            dialect,
            object,
            ordinal: 0,
        }
    }
    async fn byte(&mut self) -> Result<Option<u8>> {
        while self.offset == self.chunk.len() {
            let Some(chunk) = self.reader.next().await? else {
                return Ok(None);
            };
            if chunk.is_empty() {
                return Err(copy_error(&self.object, self.ordinal, "empty input chunk"));
            }
            self.chunk = chunk;
            self.offset = 0;
        }
        let value = self.chunk[self.offset];
        self.offset += 1;
        Ok(Some(value))
    }
    pub async fn row(&mut self) -> Result<Option<Vec<Option<String>>>> {
        let mut fields = Vec::new();
        let mut field = Vec::new();
        let mut quoted = false;
        let mut inside = false;
        let mut after = false;
        let mut escaped = false;
        let mut started = false;
        loop {
            let byte = self.byte().await?;
            if self.skip_lf {
                self.skip_lf = false;
                if byte == Some(b'\n') {
                    continue;
                }
            }
            let Some(b) = byte else {
                if inside || escaped {
                    return Err(copy_error(
                        &self.object,
                        self.ordinal,
                        "unterminated quoted field",
                    ));
                }
                if !started {
                    return Ok(None);
                }
                if fields.len() >= self.max_fields {
                    return Err(copy_error(
                        &self.object,
                        self.ordinal,
                        "too many CSV columns",
                    ));
                }
                fields.push(self.field(field, quoted)?);
                self.ordinal += 1;
                return Ok(Some(fields));
            };
            started = true;
            if inside {
                if escaped {
                    // COPY ESCAPE applies only to the quote and escape characters.
                    if b != self.dialect.quote && b != self.dialect.escape {
                        field.push(self.dialect.escape);
                    }
                    field.push(b);
                    escaped = false;
                } else if b == self.dialect.escape && b != self.dialect.quote {
                    escaped = true;
                } else if b == self.dialect.quote {
                    inside = false;
                    after = true;
                } else {
                    field.push(b);
                }
                continue;
            }
            if matches!(b, b',' | b'\n' | b'\r') && fields.len() >= self.max_fields {
                return Err(copy_error(
                    &self.object,
                    self.ordinal,
                    "too many CSV columns",
                ));
            }
            match b {
                b',' => {
                    fields.push(self.field(std::mem::take(&mut field), quoted)?);
                    quoted = false;
                    after = false;
                }
                b'\n' | b'\r' => {
                    self.skip_lf = b == b'\r';
                    fields.push(self.field(field, quoted)?);
                    self.ordinal += 1;
                    return Ok(Some(fields));
                }
                b if b == self.dialect.quote && after && self.dialect.escape == b => {
                    field.push(b);
                    inside = true;
                    after = false;
                }
                b if b == self.dialect.quote && field.is_empty() && !quoted && !after => {
                    quoted = true;
                    inside = true;
                }
                _ if after => {
                    return Err(copy_error(
                        &self.object,
                        self.ordinal,
                        "characters after closing quote",
                    ));
                }
                b if b == self.dialect.quote => {
                    return Err(copy_error(
                        &self.object,
                        self.ordinal,
                        "quote in unquoted field",
                    ));
                }
                _ => field.push(b),
            }
        }
    }
    fn field(&self, raw: Vec<u8>, quoted: bool) -> Result<Option<String>> {
        if raw.is_empty() && !quoted {
            return Ok(None);
        }
        String::from_utf8(raw)
            .map(Some)
            .map_err(|e| copy_error(&self.object, self.ordinal, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_arrow_access_core::Accessor;
    use open_arrow_access_test_support::MemoryAccessor;
    async fn parser(raw: &[u8]) -> CopyCsv {
        CopyCsv::new(
            MemoryAccessor::new(raw, 1)
                .open("test.csv".into())
                .await
                .unwrap(),
            "test.csv".into(),
            4,
            CourtListenerCopyDialect::default(),
        )
    }
    #[tokio::test]
    async fn copy_quoting_nulls_crlf_and_multiline_survive_one_byte_chunks() {
        let mut csv = parser(include_bytes!(
            "../../../testdata/courtlistener/copy-csv/quoting-nulls-multiline.csv"
        ))
        .await;
        assert_eq!(
            csv.row().await.unwrap().unwrap(),
            vec![
                Some("a".into()),
                Some("b".into()),
                Some("c".into()),
                Some("d".into())
            ]
        );
        assert_eq!(
            csv.row().await.unwrap().unwrap(),
            vec![
                None,
                Some("".into()),
                Some("line1\nline2".into()),
                Some("quote\" and slash\\".into())
            ]
        );
        assert!(csv.row().await.unwrap().is_none());
    }
    #[tokio::test]
    async fn explicit_historical_dialects_support_backticks_and_doubled_quotes() {
        let mut csv = parser(include_bytes!(
            "../../../testdata/courtlistener/copy-csv/backtick-escape.csv"
        ))
        .await;
        csv.dialect = CourtListenerCopyDialect {
            quote: b'`',
            escape: b'\\',
        };
        assert_eq!(
            csv.row().await.unwrap().unwrap(),
            vec![Some("a`b".into()), Some("".into())]
        );
        let mut csv = parser(include_bytes!(
            "../../../testdata/courtlistener/copy-csv/doubled-quotes.csv"
        ))
        .await;
        csv.dialect = CourtListenerCopyDialect {
            quote: b'"',
            escape: b'"',
        };
        assert_eq!(csv.row().await.unwrap().unwrap(), vec![Some("a\"b".into())]);
        assert!(
            CourtListenerCopyDialect {
                quote: b',',
                escape: b'"'
            }
            .validate()
            .is_err()
        );
    }
    #[tokio::test]
    async fn malformed_records_fail() {
        for raw in [
            include_bytes!("../../../testdata/courtlistener/copy-csv/unterminated-quote.csv")
                .as_slice(),
            include_bytes!("../../../testdata/courtlistener/copy-csv/characters-after-quote.csv")
                .as_slice(),
            include_bytes!("../../../testdata/courtlistener/copy-csv/unquoted-quote.csv")
                .as_slice(),
        ] {
            assert!(parser(raw).await.row().await.is_err());
        }
    }
}
