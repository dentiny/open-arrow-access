//! Provider JSON requests over the same transport used for file reads.

mod error;
pub(crate) mod files;
mod parser;
pub(crate) mod policy;
pub(crate) use parser::parse_json;
mod query;
mod url_util;
pub(crate) use url_util::resolve_origin;
mod transport;
pub(crate) use transport::{MAX_REDIRECTS, new_single_hop_transport, new_transport};
pub(crate) mod validation;

pub(crate) use error::{json_error, request_error, transport_error};
use http::Request;
use open_arrow_access_core::Result;
use open_arrow_access_storage::OpenDalAccessor;
use opendal::{Buffer, HttpBody, HttpTransporter, OperationContext};
use serde::de::DeserializeOwned;
use url::Url;

#[derive(Clone, Debug)]
pub(crate) struct HttpClient {
    pub(crate) context: OperationContext,
    pub(crate) config: open_arrow_access_storage::StorageConfig,
}

impl HttpClient {
    pub(crate) fn new() -> Result<Self> {
        HttpTransporter::install_default(new_transport()?);
        opendal::install_default();
        Ok(Self {
            context: OperationContext::new(),
            config: Default::default(),
        })
    }

    pub(crate) fn accessor(
        &self,
        endpoint: &Url,
        root: &str,
        query: Vec<(String, String)>,
    ) -> Result<OpenDalAccessor> {
        let mut context = policy::apply(&self.config, self.context.clone())?;
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
        let mut request = Self::get(url, "application/json", operation)?;
        if let Some(token) = token {
            let mut value =
                http::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|error| {
                    open_arrow_access_core::Error::new(
                        open_arrow_access_core::ErrorKind::InvalidInput,
                        operation,
                        "invalid HTTP authentication token",
                    )
                    .with_source(error)
                })?;
            value.set_sensitive(true);
            request
                .headers_mut()
                .insert(http::header::AUTHORIZATION, value);
        }
        let bytes = self.bytes(request, operation).await?;
        parse_json(&bytes, operation)
    }

    /// Construct a GET without embedding provider authentication or URL policy.
    pub(crate) fn get(url: &str, accept: &str, operation: &'static str) -> Result<Request<Buffer>> {
        Request::get(url)
            .header("accept", accept)
            .body(Buffer::new())
            .map_err(|error| request_error(operation, error))
    }

    /// Fetch one metadata or sequential-download response. Ranged storage keeps
    /// its own status handling (HEAD/206) inside OpenDAL.
    pub(crate) async fn response(
        &self,
        request: Request<Buffer>,
        operation: &'static str,
    ) -> Result<http::Response<HttpBody>> {
        let context = policy::apply(&self.config, self.context.clone())?;
        let response = context
            .http_transport()
            .fetch(request)
            .await
            .map_err(|error| transport_error(operation, error))?;
        if response.status() != http::StatusCode::OK {
            return Err(error::http_status_error(
                operation,
                response.status(),
                response.headers(),
            ));
        }
        Ok(response)
    }

    pub(crate) async fn bytes(
        &self,
        request: Request<Buffer>,
        operation: &'static str,
    ) -> Result<open_arrow_access_core::Bytes> {
        crate::http_body::collect(self.response(request, operation).await?, operation).await
    }
}
