//! Bind a provider's validated manifest to lazy HTTP file access.

use open_arrow_access_core::{Error, Result};
use open_arrow_access_storage::{OpenDalAccessor, StorageConfig};
use serde::de::DeserializeOwned;
use url::Url;

use super::{HttpClient, resolve_origin, validation::validate_relative_object};
use crate::{ResolvedFileSet, file_manifest::FileSpec};

/// Canonical download root, ordered logical-name mappings and provider metadata.
/// Providers validate identity and availability; binding validates the file namespace
/// and requires canonical relative download objects without dot segments.
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
    pub(crate) fn new(root: impl Into<String>, files: Vec<FileSpec>, metadata: M) -> Self {
        Self {
            root: root.into(),
            files,
            metadata,
            query: Vec::new(),
        }
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
    headers: http::HeaderMap,
    context: Option<opendal::OperationContext>,
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
            headers: http::HeaderMap::new(),
            context: None,
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

    /// Use provider-specific credentials on both metadata and lazy file requests.
    pub(crate) fn with_header(
        mut self,
        name: http::header::HeaderName,
        value: http::HeaderValue,
    ) -> Result<Self> {
        if self.context.is_none() {
            self.context = Some(opendal::OperationContext::new().with_http_transport(
                opendal::HttpTransporter::new(super::new_single_hop_transport()?),
            ));
        }
        // A provider selects one authentication scheme at a time.
        self.headers.clear();
        self.headers.insert(name, value);
        Ok(self)
    }

    pub(crate) fn with_context(mut self, context: opendal::OperationContext) -> Self {
        self.context = Some(context);
        self
    }

    fn client(&self) -> HttpClient {
        let mut client = self.client.clone();
        if let Some(context) = &self.context {
            client.context = context.clone();
        }
        if !self.headers.is_empty() {
            client.context = crate::common::transport::HeaderTransport::wrap(
                client.context,
                self.endpoint.clone(),
                self.headers.clone(),
            );
        }
        client
    }

    pub(crate) fn has_credentials(&self) -> bool {
        self.token.is_some() || !self.headers.is_empty()
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
        let client = self.client();
        let response = client
            .json(metadata_url.as_str(), self.token.as_deref(), self.operation)
            .await?;
        let plan = resolve(response)?;
        // Logical names are opaque aliases; only download objects become URL paths.
        for file in &plan.files {
            validate_relative_object(file.object.locator(), self.operation)?;
        }
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
            .map_err(super::error::file_namespace)
            .map_err(self.map_file_namespace_error)
    }
}

#[cfg(test)]
mod tests {
    use super::{FileSpec, HttpFileService, ResolvedFilesPlan, validate_relative_object};
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

    #[rstest::rstest]
    #[case("")]
    #[case(".")]
    #[case("..")]
    #[case("../secret")]
    #[case("nested/../secret")]
    #[case("nested/./file")]
    #[case("/absolute")]
    #[case("//host/path")]
    #[case("nested//file")]
    #[case("nested/")]
    #[case("nested\\file")]
    #[case("nul\0file")]
    fn rejects_noncanonical_download_objects(#[case] path: &str) {
        let error = validate_relative_object(path, "resolve_fixture").unwrap_err();
        assert_eq!(error.kind(), open_arrow_access_core::ErrorKind::Decode);
        assert_eq!(error.operation(), "resolve_fixture");
    }

    #[rstest::rstest]
    #[case("paper.pdf")]
    #[case("papers/第二行 #1%.pdf")]
    #[case("papers/%2e%2e/file")]
    #[case("papers/.%2E/file")]
    #[case("papers/%2Ffile")]
    #[case("papers/.../file")]
    fn keeps_literal_download_object_names(#[case] path: &str) {
        validate_relative_object(path, "resolve_fixture").unwrap();
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
        fixture.file("/files/papers/%252e%252e/file", "literal");
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
                Ok(ResolvedFilesPlan::new(
                    "files/",
                    vec![
                        file("../opaque", "shared", Some(4)),
                        file("alias", "shared", Some(4)),
                        file("unknown length", "unknown", None),
                        file("encoded-looking name", "papers/%2e%2e/file", Some(7)),
                    ],
                    metadata,
                ))
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
            [
                "../opaque",
                "alias",
                "unknown length",
                "encoded-looking name"
            ]
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
        let reader = files.open("encoded-looking name".into()).await.unwrap();
        assert_eq!(read_all(reader, "test").await.unwrap(), b"literal");
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
