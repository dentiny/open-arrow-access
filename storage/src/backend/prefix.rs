//! Open a bounded prefix through the backend's native range operation.
//!
//! The backend receives the range before constructing or signing its request.
//! HTTP response validation is separate from backend-independent range I/O.

use crate::error::invalid_response;
use http::{Method, Request, Response, StatusCode, header};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext,
    raw::{
        Layer, OpRead, RpRead, Service, Servicer, normalize_path, oio, parse_content_range,
        validate_path,
    },
};

/// Use the layered service so retries, timeouts, and transport admission remain
/// active. The caller uses response metadata to accept a whole object shorter
/// than the requested range without triggering OpenDAL's exact-size EOF check.
pub(super) async fn open(
    operator: &opendal::Operator,
    path: &str,
    size: usize,
) -> opendal::Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
    use oio::Read as _;

    let path = normalize_path(path);
    if !validate_path(&path, opendal::EntryMode::FILE) {
        return Err(opendal::Error::new(
            opendal::ErrorKind::IsADirectory,
            "read path is a directory",
        ));
    }
    let reader = operator
        .service()
        .read(operator.context(), &path, OpRead::default())?;
    reader.open((0..size as u64).into()).await
}

#[derive(Debug)]
pub(super) struct HttpPrefixValidationLayer(pub usize);

impl Layer for HttpPrefixValidationLayer {
    fn apply_context(&self, _: Servicer, context: OperationContext) -> OperationContext {
        let transport = HttpTransporter::new(PrefixTransport {
            inner: context.http_transport().clone(),
            size: self.0 as u64,
        });
        context.with_http_transport(transport)
    }
}

struct PrefixTransport {
    inner: HttpTransporter,
    size: u64,
}

impl HttpTransport for PrefixTransport {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        // Only inspect the prefix operation. Authentication and other auxiliary
        // requests, as well as non-HTTP backends, retain their own behavior.
        let expected_range = format!("bytes=0-{}", self.size - 1);
        if request.method() != Method::GET
            || !request
                .headers()
                .get(header::RANGE)
                .is_some_and(|range| range == expected_range.as_str())
        {
            return self.inner.fetch(request).await;
        }
        let mut response = self.inner.fetch(request).await?;
        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE
            && response
                .headers()
                .get(header::CONTENT_RANGE)
                .is_some_and(|value| value == "bytes */0")
        {
            // Only an explicit zero total proves an empty object. Drain the
            // error response before replacing it so its HTTP permit is released.
            response.body_mut().to_buffer().await?;
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, 0)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap());
        }
        let mut short_length = None;
        if response.status() == StatusCode::PARTIAL_CONTENT {
            let range = parse_content_range(response.headers())?.ok_or_else(invalid_response)?;
            let len = range.size().ok_or_else(invalid_response)?;
            if range.range() != Some(0..len.min(self.size)) {
                return Err(invalid_response());
            }
            if len < self.size {
                short_length = Some(len);
            }
        } else if response.status() == StatusCode::OK {
            // A server ignoring Range is usable only if its entire body fits
            // the requested prefix. Never turn a bootstrap into a full download.
            let length = opendal::raw::parse_content_length(response.headers())?;
            if !length.is_some_and(|length| length <= self.size) {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unsupported,
                    "HTTP server ignored the bootstrap byte range",
                ));
            }
            short_length = length.filter(|length| *length < self.size);
        }
        if let Some(length) = short_length {
            // The caller cannot poll the bounded service stream to EOF for a
            // short object: CompleteReader would reject its legal short range.
            // Drain and validate here, before that wrapper, retaining at most
            // the requested prefix and releasing transport permits normally.
            use opendal::raw::oio::ReadStream as _;
            let mut buffers = Vec::new();
            let mut received = 0;
            loop {
                let buffer = response.body_mut().read().await?;
                if buffer.is_empty() {
                    break;
                }
                if buffer.len() as u64 > length - received {
                    return Err(invalid_response());
                }
                received += buffer.len() as u64;
                buffers.push(buffer);
            }
            if received != length {
                return Err(invalid_response());
            }
            let buffer: Buffer = buffers.into_iter().flatten().collect();
            *response.body_mut() = HttpBody::new(
                futures::stream::once(std::future::ready(Ok(buffer))),
                Some(length),
            );
        }
        Ok(response)
    }
}
