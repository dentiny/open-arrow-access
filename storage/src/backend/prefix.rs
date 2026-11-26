//! Bound the first HTTP GET while keeping OpenDAL's stream non-exact.
//!
//! An exact `0..chunk` stream rejects a legal short 206 for a smaller file.
//! Adding the range at the transport boundary lets us validate against the
//! response's whole-object length instead, without bypassing operator policies.

use http::{Method, Request, Response, StatusCode, header};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext,
    raw::{Layer, Servicer, parse_content_range},
};

#[derive(Debug)]
pub(super) struct PrefixLayer(pub usize);

impl Layer for PrefixLayer {
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
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        // Only installed on a temporary, unchunked HTTP reader. Other methods
        // and explicit ranges retain the original operator's behavior.
        if request.method() != Method::GET || request.headers().contains_key(header::RANGE) {
            return self.inner.fetch(request).await;
        }
        request.headers_mut().insert(
            header::RANGE,
            format!("bytes=0-{}", self.size - 1)
                .parse()
                .expect("numeric byte range"),
        );
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
        if response.status() == StatusCode::PARTIAL_CONTENT {
            let range = parse_content_range(response.headers())?.ok_or_else(invalid_response)?;
            let len = range.size().ok_or_else(invalid_response)?;
            if range.range() != Some(0..len.min(self.size)) {
                return Err(invalid_response());
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
        }
        Ok(response)
    }
}

fn invalid_response() -> opendal::Error {
    opendal::Error::new(
        opendal::ErrorKind::Unexpected,
        "invalid bootstrap Content-Range",
    )
}
