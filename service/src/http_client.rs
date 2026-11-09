//! Provider JSON requests over the same transport used for file reads.

mod error;
pub(crate) mod files;
mod parser;
mod query;
mod url_util;
pub(crate) use url_util::resolve_origin;
mod transport;
pub(crate) use transport::{MAX_REDIRECTS, new_single_hop_transport, new_transport};
pub(crate) mod validation;

pub(crate) use error::{http_status_error, json_error, request_error, transport_error};
use http::Request;
use open_arrow_access_core::Result;
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
        HttpTransporter::install_default(new_transport()?);
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
        let request = request
            .body(Buffer::new())
            .map_err(|error| request_error(operation, error))?;
        let response = self
            .context
            .http_transport()
            .fetch(request)
            .await
            .map_err(|error| transport_error(operation, error))?;
        let status = response.status();
        if status != http::StatusCode::OK {
            return Err(error::http_status_error(
                operation,
                status,
                response.headers(),
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
        parser::parse_json(&bytes, operation)
    }
}
