//! Response-body framing shared by buffered metadata and sequential downloads.
mod error;
mod length;
mod validation;
use http::Response;
use length::BodyLength;
use open_arrow_access_core::{Bytes, Result};
use opendal::{Buffer, HttpBody, raw::oio::ReadStream};

const MAX_INITIAL_BODY_CAPACITY: u64 = 64 * 1024 * 1024;

pub(crate) async fn collect(body: BodyReader) -> Result<Bytes> {
    collect_limited(body, None).await
}

pub(crate) async fn collect_limited(mut body: BodyReader, limit: Option<usize>) -> Result<Bytes> {
    let Some(first) = body.next().await? else {
        return Ok(Bytes::new());
    };
    let operation = body.length.operation;
    validation::validate_collected_bytes(0, first.len(), limit, operation)?;
    // Validate EOF before returning a single buffer, preserving framing errors.
    let Some(second) = body.next().await? else {
        return Ok(first.to_bytes());
    };
    validation::validate_collected_bytes(first.len(), second.len(), limit, operation)?;
    // Content-Length is an allocation hint, never an unbounded reservation.
    // Wait for actual data before reserving for a possibly stale large header.
    let capacity = body
        .content_length()
        .unwrap_or(0)
        .min(MAX_INITIAL_BODY_CAPACITY) as usize;
    let mut bytes = Vec::with_capacity(capacity.min(limit.unwrap_or(usize::MAX)));
    for buffer in [first, second] {
        for part in buffer {
            bytes.extend_from_slice(&part);
        }
    }
    while let Some(buffer) = body.next().await? {
        validation::validate_collected_bytes(bytes.len(), buffer.len(), limit, operation)?;
        for part in buffer {
            bytes.extend_from_slice(&part);
        }
    }
    Ok(bytes.into())
}

/// Read and validate response framing for both buffered and streaming consumers.
pub(crate) struct BodyReader {
    body: HttpBody,
    length: BodyLength,
}

impl BodyReader {
    pub(crate) fn new(response: Response<HttpBody>, operation: &'static str) -> Result<Self> {
        let length = BodyLength::new(response.headers(), operation)?;
        Ok(Self {
            body: response.into_body(),
            length,
        })
    }

    /// Require exact framing even when the server omits Content-Length.
    pub(crate) fn with_expected_length(mut self, expected: u64) -> Result<Self> {
        if *self.length.expected.get_or_insert(expected) != expected {
            return Err(error::body_error(
                self.length.operation,
                "unexpected Content-Length",
            ));
        }
        Ok(self)
    }

    pub(crate) fn content_length(&self) -> Option<u64> {
        self.length.expected
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Buffer>> {
        let buffer =
            self.body.read().await.map_err(|error| {
                crate::http_client::transport_error(self.length.operation, error)
            })?;
        if buffer.is_empty() {
            self.length.finish()?;
            return Ok(None);
        }
        self.length.push(buffer.len())?;
        Ok(Some(buffer))
    }
}
