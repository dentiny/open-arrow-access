//! Open a bounded prefix through the backend's native range operation.
//!
//! The backend receives the range before constructing or signing its request.
//! HTTP response validation is separate from backend-independent range I/O.

use crate::validation::{self, HttpPrefixResponse};
use http::{Method, Request, Response, StatusCode, header};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext,
    raw::{Layer, OpRead, RpRead, Service, Servicer, normalize_path, oio},
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
    validation::validate_read_path(&path)?;
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
        let response_kind = validation::validate_http_prefix_response(
            response.status(),
            response.headers(),
            self.size,
        )?;
        if matches!(response_kind, HttpPrefixResponse::Empty) {
            // Drain before replacing the response to release its HTTP permit.
            response.body_mut().to_buffer().await?;
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_LENGTH, 0)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap());
        }
        if let HttpPrefixResponse::Short(length) = response_kind {
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
                validation::validate_prefix_chunk_length(buffer.len() as u64, length - received)?;
                received += buffer.len() as u64;
                buffers.push(buffer);
            }
            validation::validate_prefix_read_length(received, length)?;
            let buffer: Buffer = buffers.into_iter().flatten().collect();
            *response.body_mut() = HttpBody::new(
                futures::stream::once(std::future::ready(Ok(buffer))),
                Some(length),
            );
        }
        Ok(response)
    }
}
