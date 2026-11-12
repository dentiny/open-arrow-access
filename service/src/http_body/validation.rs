use super::error::body_error;
use http::HeaderMap;
use open_arrow_access_core::Result;

pub(super) struct BodyLength {
    pub(super) expected: Option<u64>,
    read: u64,
    pub(super) operation: &'static str,
}

impl BodyLength {
    pub(super) fn new(headers: &HeaderMap, operation: &'static str) -> Result<Self> {
        let expected = headers
            .get("content-length")
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| body_error(operation, "invalid Content-Length"))
            })
            .transpose()?;
        Ok(Self {
            expected,
            read: 0,
            operation,
        })
    }

    pub(super) fn push(&mut self, bytes: usize) -> Result<()> {
        self.read = self
            .read
            .checked_add(bytes as u64)
            .ok_or_else(|| body_error(self.operation, "body length overflow"))?;
        if self.expected.is_some_and(|length| self.read > length) {
            return Err(body_error(
                self.operation,
                "HTTP body exceeds Content-Length",
            ));
        }
        Ok(())
    }

    pub(super) fn finish(&self) -> Result<()> {
        if self.expected.is_some_and(|length| self.read != length) {
            return Err(body_error(self.operation, "truncated HTTP body"));
        }
        Ok(())
    }
}
