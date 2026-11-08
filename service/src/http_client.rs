//! Bounded provider JSON requests over the same transport used for file reads.

mod error;

use error::transport_error;
use http::{Request, StatusCode};
use open_arrow_access_core::{Error, ErrorKind, Result};
use opendal::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, raw::oio::ReadStream,
};
use serde::de::DeserializeOwned;

#[derive(Clone, Debug)]
pub(crate) struct HttpClient {
    pub(crate) context: OperationContext,
}

impl HttpClient {
    pub(crate) fn new() -> Self {
        opendal::install_default();
        let context = OperationContext::new();
        Self {
            context: context.with_http_transport(HttpTransporter::new(IdentifiedTransport(
                context.http_transport().clone(),
            ))),
        }
    }

    pub(crate) async fn json<T: DeserializeOwned>(
        &self,
        url: &str,
        token: Option<&str>,
        limit: usize,
        operation: &'static str,
    ) -> Result<T> {
        let mut request = Request::get(url).header("accept", "application/json");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let request = request.body(Buffer::new()).map_err(|error| {
            Error::new(ErrorKind::InvalidInput, operation, "invalid HTTP request")
                .with_source(error)
        })?;
        let response = self
            .context
            .http_transport()
            .fetch(request)
            .await
            .map_err(|error| transport_error(operation, error))?;
        let status = response.status();
        if status != StatusCode::OK {
            // TODO: Extract shared HTTP status-to-error translation.
            let kind = match status {
                StatusCode::NOT_FOUND => opendal::ErrorKind::NotFound,
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    opendal::ErrorKind::PermissionDenied
                }
                StatusCode::TOO_MANY_REQUESTS => opendal::ErrorKind::RateLimited,
                _ => opendal::ErrorKind::Unexpected,
            };
            return Err(transport_error(
                operation,
                opendal::Error::new(kind, format!("metadata HTTP {status}")),
            ));
        }
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            let buffer = body
                .read()
                .await
                .map_err(|error| transport_error(operation, error))?;
            if buffer.is_empty() {
                break;
            }
            if buffer.len() > limit - bytes.len() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    operation,
                    "metadata exceeds configured byte limit",
                ));
            }
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        serde_json::from_slice(&bytes).map_err(|error| {
            Error::new(ErrorKind::Decode, operation, "invalid JSON metadata").with_source(error)
        })
    }
}

// Apply identification to metadata and operator requests using this context.
#[derive(Debug)]
struct IdentifiedTransport(HttpTransporter);

impl HttpTransport for IdentifiedTransport {
    async fn fetch(
        &self,
        mut request: Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        request
            .headers_mut()
            .entry("user-agent")
            .or_insert(http::HeaderValue::from_static(concat!(
                "open-arrow-access/",
                env!("CARGO_PKG_VERSION")
            )));
        self.0.fetch(request).await
    }
}
