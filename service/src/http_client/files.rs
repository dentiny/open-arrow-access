//! Bind a provider's validated manifest to lazy HTTP file access.

use http::{HeaderMap, header::AUTHORIZATION};
use open_arrow_access_core::{Error, Result};
use open_arrow_access_storage::{OpenDalAccessor, StorageConfig};
use opendal::{HttpTransporter, OperationContext};
use serde::de::DeserializeOwned;
use url::Url;

use super::{HttpClient, resolve_origin};
use crate::{ResolvedFileSet, file_manifest::FileSpec};

/// Canonical download root, ordered logical-name mappings and provider metadata.
/// Providers validate identity, availability and names before returning this plan.
pub(crate) struct ResolvedFilesPlan<M> {
    /// Download path prefix relative to the service's HTTP origin.
    root: String,
    files: Vec<FileSpec>,
    metadata: M,
    /// (Query parameter name, value) pairs appended to download requests and
    /// URL-encoded by the transport; metadata requests keep their own query.
    query: Vec<(String, String)>,
}

impl<M> ResolvedFilesPlan<M> {
    pub(crate) fn new(root: impl Into<String>, files: Vec<FileSpec>, metadata: M) -> Result<Self> {
        open_arrow_access_storage::validate_file_names(files.iter().map(|file| file.name.as_str()))
            .map_err(super::error::file_namespace)?;
        Ok(Self {
            root: root.into(),
            files,
            metadata,
            query: Vec::new(),
        })
    }

    pub(crate) fn with_query(mut self, query: &[(&str, &str)]) -> Self {
        self.query = query
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect();
        self
    }
}

/// Connection and error context shared by JSON-manifest providers.
#[derive(Clone)]
pub(crate) struct HttpFileService {
    endpoint: Url,
    client: HttpClient,
    token: Option<String>,
    headers: HeaderMap,
    single_hop_context: Option<OperationContext>,
    /// Provider operation for metadata HTTP/JSON errors.
    operation: &'static str,
    /// Wrap namespace errors with provider context.
    map_file_namespace_error: fn(Error) -> Error,
}

impl std::fmt::Debug for HttpFileService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpFileService")
            .field("endpoint", &self.endpoint)
            .field("token", &self.token.as_ref().map(|_| "<token>"))
            .field("operation", &self.operation)
            .finish_non_exhaustive()
    }
}

impl HttpFileService {
    pub(crate) fn new(
        endpoint: &str,
        configure: &'static str,
        operation: &'static str,
        map_file_namespace_error: fn(Error) -> Error,
    ) -> Result<Self> {
        Ok(Self {
            endpoint: resolve_origin(endpoint, configure)?,
            client: HttpClient::new()?,
            token: None,
            headers: HeaderMap::new(),
            single_hop_context: None,
            operation,
            map_file_namespace_error,
        })
    }

    pub(crate) fn with_http_config(mut self, config: StorageConfig) -> Result<Self> {
        config.validate()?;
        self.client.config = config;
        Ok(self)
    }

    pub(crate) fn with_token(mut self, token: &str) -> Result<Self> {
        super::validation::validate_empty_token(token, self.operation)?;
        super::validation::bearer_header(token, self.operation)?;
        self.token = Some(token.to_owned());
        Ok(self)
    }

    /// Add provider-specific credentials to metadata and all lazy byte reads.
    /// The provider validates credential syntax before constructing this value.
    pub(crate) fn with_authorization(mut self, token: &str) -> Result<Self> {
        let mut value = http::HeaderValue::from_str(token)
            .map_err(|source| super::error::invalid_token(self.operation, source))?;
        value.set_sensitive(true);
        self.headers.insert(AUTHORIZATION, value);
        if self.single_hop_context.is_none() {
            self.single_hop_context = Some(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(super::new_single_hop_transport()?)),
            );
        }
        Ok(self)
    }

    /// Custom transports must return single-hop responses.
    pub(crate) fn with_context(mut self, context: OperationContext) -> Self {
        self.single_hop_context = Some(context);
        self
    }

    pub(crate) fn has_auth_headers(&self) -> bool {
        !self.headers.is_empty()
    }

    pub(crate) fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Resolve a JSON manifest without fetching file bodies.
    pub(crate) async fn resolve_files<T: DeserializeOwned, M>(
        &self,
        metadata_url: &Url,
        resolve: impl FnOnce(T) -> Result<ResolvedFilesPlan<M>>,
    ) -> Result<ResolvedFileSet<OpenDalAccessor, M>> {
        let mut client = self.client.clone();
        if let Some(context) = self.single_hop_context.clone() {
            let context = crate::common::transport::follow_redirects(context);
            let inner = context.http_transport().clone();
            client.context = context.with_http_transport(HttpTransporter::new(HeaderTransport {
                inner,
                origin: self.endpoint.clone(),
                headers: self.headers.clone(),
            }));
        }
        let response = client
            .json(metadata_url.as_str(), self.token.as_deref(), self.operation)
            .await?;
        let plan = resolve(response)?;
        let lengths = plan.files.iter().filter_map(|file| {
            file.content_length
                .map(|length| (file.object.locator().to_owned(), length))
        });
        let storage = client
            .accessor(
                &self.endpoint,
                &plan.root,
                plan.query,
                self.token.as_deref(),
            )?
            .with_content_lengths(lengths);
        ResolvedFileSet::from_files(storage, plan.files, plan.metadata)
            .map_err(self.map_file_namespace_error)
    }
}

/// Attach credentials through the existing origin-scoped redirect transport.
/// OpenDAL's HEAD and Range requests use this same transport.
#[derive(Debug)]
struct HeaderTransport {
    inner: HttpTransporter,
    origin: Url,
    headers: HeaderMap,
}

impl opendal::HttpTransport for HeaderTransport {
    async fn fetch(
        &self,
        mut request: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<opendal::HttpBody>> {
        request
            .extensions_mut()
            .insert(crate::common::transport::AuthScope {
                origin: self.origin.clone(),
                headers: self.headers.clone(),
                query: Vec::new(),
            });
        self.inner.fetch(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::{FileSpec, HttpFileService, ResolvedFilesPlan};
    use futures::TryStreamExt;
    use open_arrow_access_core::{Accessor, ByteAccessor, ListRequest, read_all};
    use open_arrow_access_test_support::{fixture, http::HttpFixture};

    fn file(name: &str, object: &str, length: Option<u64>) -> FileSpec {
        FileSpec {
            name: name.into(),
            object: object.into(),
            content_length: length,
        }
    }

    #[tokio::test]
    async fn ordered_aliases_share_object_lengths_and_keep_provider_metadata() {
        let fixture = HttpFixture::new();
        fixture.respond(
            "/manifest",
            http::StatusCode::OK,
            fixture!("federal-register", "document.json").await,
        );
        fixture.file("/files/shared", "data");
        fixture.file("/files/unknown", "other");
        let service = HttpFileService::new(
            fixture.endpoint(),
            "configure_fixture",
            "resolve_fixture",
            |error| error,
        )
        .unwrap();
        let url = service.endpoint().join("manifest").unwrap();
        let files = service
            .resolve_files(&url, |metadata: serde_json::Value| {
                ResolvedFilesPlan::new(
                    "files/",
                    vec![
                        file("../opaque", "shared", Some(4)),
                        file("alias", "shared", Some(4)),
                        file("unknown length", "unknown", None),
                    ],
                    metadata,
                )
            })
            .await
            .unwrap();
        assert_eq!(files.metadata()["unknown"], "retained");
        let listed = files
            .list(ListRequest::new(""))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|object| object.locator())
                .collect::<Vec<_>>(),
            ["../opaque", "alias", "unknown length"]
        );
        assert_eq!(
            fixture.requests().len(),
            1,
            "resolution and listing never open attachments"
        );
        for name in ["../opaque", "alias"] {
            let mut reader = files.open(name.into()).await.unwrap();
            assert!(reader.capability().read_with_range);
            assert_eq!(reader.metadata().content_length, Some(4));
            assert_eq!(reader.read_range(1..3).await.unwrap().as_ref(), b"at");
            assert_eq!(read_all(reader, "test").await.unwrap(), b"data");
        }
        let reader = files.open("unknown length".into()).await.unwrap();
        assert_eq!(reader.metadata().content_length, Some(5));
        assert_eq!(read_all(reader, "test").await.unwrap(), b"other");
        let requests = fixture.requests();
        assert!(
            !requests
                .iter()
                .any(|request| request.method == http::Method::HEAD
                    && request.path == "/files/shared"),
            "trusted lengths are keyed by download object, not logical alias"
        );
        assert!(requests.iter().any(
            |request| request.method == http::Method::HEAD && request.path == "/files/unknown"
        ));
    }
}
