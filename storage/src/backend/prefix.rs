//! Open a bounded prefix through the backend's native range operation.
//!
//! The backend receives the range before constructing or signing its request.
//! HTTP response validation is separate from backend-independent range I/O.

use crate::validation::{self, HttpPrefixResponse};
use http::{Method, Request, Response, StatusCode, header};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, Metadata, OperationContext, Operator,
    raw::{Layer, OpRead, Service, Servicer, normalize_path, oio},
};

/// Owns a bootstrap response until its prefix is consumed or dropped.
/// Native range I/O preserves the operator's policies and backend signing.
pub(super) struct PrefixReader {
    metadata: Metadata,
    stream: Box<dyn oio::ReadStreamDyn>,
    size: u64,
}

impl PrefixReader {
    /// None means the backend needs the known-length fallback: either it rejects
    /// an oversized prefix or does not return metadata while opening a range.
    pub(super) async fn open(
        operator: Operator,
        path: &str,
        size: usize,
    ) -> opendal::Result<Option<Self>> {
        use oio::Read as _;

        let operator = operator.layer(HttpPrefixValidationLayer(size));
        let path = normalize_path(path);
        validation::validate_read_path(&path)?;
        let reader = operator
            .service()
            .read(operator.context(), &path, OpRead::default())?;
        let (response, stream) = match reader.open((0..size as u64).into()).await {
            Ok(response) => response,
            Err(error) if error.kind() == opendal::ErrorKind::RangeNotSatisfied => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(response.into_metadata().map(|metadata| Self {
            metadata,
            stream,
            size: size as u64,
        }))
    }

    pub(super) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Consuming self releases the response before subsequent ranges are opened.
    pub(super) async fn read(mut self) -> opendal::Result<Buffer> {
        let expected = self.metadata.content_length().min(self.size);
        Self::collect(&mut self.stream, expected, expected == self.size).await
    }

    // Short service streams cannot be polled to EOF: OpenDAL would enforce the
    // original, larger requested range there. HTTP bodies can always be drained.
    async fn collect(
        stream: &mut impl oio::ReadStream,
        expected: u64,
        drain_to_eof: bool,
    ) -> opendal::Result<Buffer> {
        let mut buffers = Vec::new();
        let mut received = 0;
        while drain_to_eof || received < expected {
            let buffer = stream.read().await?;
            if buffer.is_empty() {
                break;
            }
            validation::validate_prefix_chunk_length(buffer.len() as u64, expected - received)?;
            received += buffer.len() as u64;
            buffers.push(buffer);
        }
        validation::validate_prefix_read_length(received, expected)?;
        Ok(buffers.into_iter().flatten().collect())
    }
}

#[derive(Debug)]
struct HttpPrefixValidationLayer(usize);

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
            let buffer = PrefixReader::collect(response.body_mut(), length, true).await?;
            *response.body_mut() = HttpBody::new(
                futures::stream::once(std::future::ready(Ok(buffer))),
                Some(length),
            );
        }
        Ok(response)
    }
}
