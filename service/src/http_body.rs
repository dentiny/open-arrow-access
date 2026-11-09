//! Response-body framing shared by buffered metadata and sequential downloads.
use http::{HeaderMap, Response};
use open_arrow_access_core::{Bytes, Error, ErrorKind, Result};
use opendal::{HttpBody, raw::oio::ReadStream};

pub(crate) async fn collect(
    response: Response<HttpBody>,
    operation: &'static str,
) -> Result<Bytes> {
    let mut length = BodyLength::new(response.headers(), operation)?;
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    loop {
        let buffer = body
            .read()
            .await
            .map_err(|error| crate::http_client::transport_error(operation, error))?;
        if buffer.is_empty() {
            break;
        }
        length.push(buffer.len())?;
        for part in buffer {
            bytes.extend_from_slice(&part);
        }
    }
    length.finish()?;
    Ok(bytes.into())
}

pub(crate) struct BodyLength {
    pub(crate) expected: Option<u64>,
    read: u64,
    operation: &'static str,
}
impl BodyLength {
    pub(crate) fn new(headers: &HeaderMap, operation: &'static str) -> Result<Self> {
        let expected = headers
            .get("content-length")
            .map(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Decode, operation, "invalid Content-Length")
                    })
            })
            .transpose()?;
        Ok(Self {
            expected,
            read: 0,
            operation,
        })
    }
    fn error(&self, message: &str) -> Error {
        Error::new(ErrorKind::Decode, self.operation, message)
    }
    pub(crate) fn push(&mut self, bytes: usize) -> Result<()> {
        self.read = self
            .read
            .checked_add(bytes as u64)
            .ok_or_else(|| self.error("body length overflow"))?;
        if self.expected.is_some_and(|length| self.read > length) {
            return Err(self.error("HTTP body exceeds Content-Length"));
        }
        Ok(())
    }
    pub(crate) fn finish(&self) -> Result<()> {
        if self.expected.is_some_and(|length| self.read != length) {
            return Err(self.error("truncated HTTP body"));
        }
        Ok(())
    }
}
