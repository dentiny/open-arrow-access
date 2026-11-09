//! Response-body consumption shared by typed metadata and sequential downloads.
use http::HeaderMap;
use open_arrow_access_core::{Bytes, Error, ErrorKind, Result};
use opendal::{HttpBody, raw::oio::ReadStream};

/// Track the declared representation length independently of transport hints.
/// The same checks apply to buffered metadata and lazy streaming reads.
pub(crate) struct BodyLength {
    expected: Option<u64>,
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
                    .ok_or_else(|| decode(operation, "invalid Content-Length"))
            })
            .transpose()?;
        Ok(Self {
            expected,
            read: 0,
            operation,
        })
    }

    pub(crate) fn expected(&self) -> Option<u64> {
        self.expected
    }

    pub(crate) fn push(&mut self, bytes: usize) -> Result<()> {
        self.read = self
            .read
            .checked_add(bytes as u64)
            .ok_or_else(|| decode(self.operation, "body length overflow"))?;
        if self.expected.is_some_and(|length| self.read > length) {
            return Err(decode(self.operation, "HTTP body exceeds Content-Length"));
        }
        Ok(())
    }

    pub(crate) fn finish(&self) -> Result<()> {
        if self.expected.is_some_and(|length| self.read != length) {
            return Err(decode(self.operation, "truncated HTTP body"));
        }
        Ok(())
    }
}

fn decode(operation: &'static str, message: &str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

pub(crate) async fn collect_body(
    response: http::Response<HttpBody>,
    operation: &'static str,
) -> Result<Bytes> {
    let mut length = BodyLength::new(response.headers(), operation)?;
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    loop {
        let buffer = body
            .read()
            .await
            .map_err(|error| super::transport_error(operation, error))?;
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
