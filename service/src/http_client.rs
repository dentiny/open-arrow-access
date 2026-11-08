//! Provider JSON requests over the same transport used for file reads.

mod error;
pub(crate) mod files;
mod query;
pub(crate) mod validation;

use error::transport_error;
use http::{Request, StatusCode};
use open_arrow_access_core::{Error, ErrorKind, Result};
use open_arrow_access_storage::OpenDalAccessor;
use opendal::{Buffer, HttpTransporter, OperationContext, raw::oio::ReadStream};
use serde::de::DeserializeOwned;
use url::Url;

#[derive(Clone, Debug)]
pub(crate) struct HttpClient {
    pub(crate) context: OperationContext,
}

impl HttpClient {
    pub(crate) fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("open-arrow-access/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| {
                Error::new(
                    ErrorKind::Io,
                    "configure_http",
                    "failed to build HTTP client",
                )
                .with_source(error)
            })?;
        HttpTransporter::install_default(opendal_http_transport_reqwest::ReqwestTransport::new(
            client,
        ));
        opendal::install_default();
        Ok(Self {
            context: OperationContext::new(),
        })
    }

    pub(crate) fn accessor(
        &self,
        endpoint: &Url,
        root: &str,
        query: Vec<(String, String)>,
    ) -> Result<OpenDalAccessor> {
        let mut context = self.context.clone();
        if !query.is_empty() {
            context = context.with_http_transport(HttpTransporter::new(query::QueryTransport {
                inner: context.http_transport().clone(),
                query,
            }));
        }
        let operator = opendal::Operator::new(
            opendal_service_http::Http::default()
                .endpoint(endpoint.as_str().trim_end_matches('/'))
                .root(root),
        )
        .map_err(|error| transport_error("configure_http", error))?
        .with_context(context);
        Ok(OpenDalAccessor::new(operator))
    }

    pub(crate) async fn json<T: DeserializeOwned>(
        &self,
        url: &str,
        token: Option<&str>,
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
            for part in buffer {
                bytes.extend_from_slice(&part);
            }
        }
        serde_json::from_slice(&bytes).map_err(|error| {
            Error::new(ErrorKind::Decode, operation, "invalid JSON metadata").with_source(error)
        })
    }
}
