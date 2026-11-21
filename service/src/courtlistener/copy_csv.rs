//! Streaming PostgreSQL COPY CSV framing, retaining quoted-empty vs unquoted NULL.
use open_arrow_access_core::{ByteReader, Bytes, Error, ErrorKind, Result};

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
impl CourtListenerCopyDialect {
    pub(crate) fn validate(self) -> Result<()> {
        for byte in [self.quote, self.escape] {
            if !byte.is_ascii_graphic() || matches!(byte, b',' | b'\r' | b'\n') {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "courtlistener_copy",
                    "invalid COPY quote/escape character",
                ));
            }
        }
        Ok(())
    }
}

pub(super) struct CopyCsv {
    reader: Box<dyn ByteReader>,
    chunk: Bytes,
    offset: usize,
    skip_lf: bool,
    limit: usize,
    max_fields: usize,
    dialect: CourtListenerCopyDialect,
    pub object: String,
    pub ordinal: u64,
}
impl CopyCsv {
    pub fn new(
        reader: Box<dyn ByteReader>,
        object: String,
        limit: usize,
        max_fields: usize,
        dialect: CourtListenerCopyDialect,
    ) -> Self {
        Self {
            reader,
            chunk: Bytes::new(),
            offset: 0,
            skip_lf: false,
            limit,
            max_fields,
            dialect,
            object,
            ordinal: 0,
        }
    }
    fn error(&self, message: impl std::fmt::Display) -> Error {
        Error::new(
            ErrorKind::Decode,
            "courtlistener_copy",
            format!("{} record {}: {message}", self.object, self.ordinal),
        )
    }
    async fn byte(&mut self) -> Result<Option<u8>> {
        while self.offset == self.chunk.len() {
            let Some(chunk) = self.reader.next().await? else {
                return Ok(None);
            };
            if chunk.is_empty() {
                return Err(self.error("empty input chunk"));
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
        let mut size = 0;
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
                    return Err(self.error("unterminated quoted field"));
                }
                if size == 0 {
                    return Ok(None);
                }
                if fields.len() >= self.max_fields {
                    return Err(self.error("too many CSV columns"));
                }
                fields.push(self.field(field, quoted)?);
                self.ordinal += 1;
                return Ok(Some(fields));
            };
            size += 1;
            if size > self.limit {
                return Err(self.error("record exceeds max_record_bytes"));
            }
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
                return Err(self.error("too many CSV columns"));
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
                _ if after => return Err(self.error("characters after closing quote")),
                b if b == self.dialect.quote => return Err(self.error("quote in unquoted field")),
                _ => field.push(b),
            }
        }
    }
    fn field(&self, raw: Vec<u8>, quoted: bool) -> Result<Option<String>> {
        if raw.is_empty() && !quoted {
            return Ok(None);
        }
        String::from_utf8(raw).map(Some).map_err(|e| self.error(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_arrow_access_core::Accessor;
    use open_arrow_access_test_support::MemoryAccessor;
    async fn parser(raw: &[u8], limit: usize) -> CopyCsv {
        CopyCsv::new(
            MemoryAccessor::new(raw, 1)
                .open("test.csv".into())
                .await
                .unwrap(),
            "test.csv".into(),
            limit,
            4,
            CourtListenerCopyDialect::default(),
        )
    }
    #[tokio::test]
    async fn copy_quoting_nulls_crlf_and_multiline_survive_one_byte_chunks() {
        let mut csv = parser(
            b"a,b,c,d\r\n,\"\",\"line1\nline2\",\"quote\\\" and slash\\\\\"\r\n",
            1024,
        )
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
        let mut csv = parser(b"`a\\`b`,``\n", 1024).await;
        csv.dialect = CourtListenerCopyDialect {
            quote: b'`',
            escape: b'\\',
        };
        assert_eq!(
            csv.row().await.unwrap().unwrap(),
            vec![Some("a`b".into()), Some("".into())]
        );
        let mut csv = parser(b"\"a\"\"b\"", 1024).await;
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
    async fn malformed_and_oversized_records_fail() {
        for raw in [
            b"\"unterminated".as_slice(),
            b"\"closed\"garbage\n",
            b"a\"b\n",
        ] {
            assert!(parser(raw, 1024).await.row().await.is_err());
        }
        assert!(parser(b"abcdefgh\n", 4).await.row().await.is_err());
    }
}
