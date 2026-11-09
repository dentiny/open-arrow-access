//! Bind a provider's validated manifest to lazy HTTP file access.

use open_arrow_access_core::{Error, Result};
use open_arrow_access_storage::{NamedFileAccessor, OpenDalAccessor};
use serde::de::DeserializeOwned;
use url::Url;

use super::{HttpClient, resolve_origin};
use crate::{ResolvedFileSet, file_manifest::FileSpec};

/// Canonical download root, ordered logical-name mappings and provider metadata.
/// Providers validate identity, availability and names before returning this plan.
pub(crate) struct ResolvedFilesPlan<M> {
    /// Download path prefix relative to the service's HTTP origin.
    root: String,
    /// Logical names, download objects and optional trusted representation lengths.
    /// Insertion order is manifest/listing order; empty and duplicate names fail.
    files: Vec<FileSpec>,
    /// Provider metadata retained alongside the resolved file namespace.
    metadata: M,
    /// (Query parameter name, value) pairs appended to download requests and
    /// URL-encoded by the transport; metadata requests keep their own query.
    query: Vec<(String, String)>,
}

impl<M> ResolvedFilesPlan<M> {
    pub(crate) fn new(root: impl Into<String>, files: Vec<FileSpec>, metadata: M) -> Result<Self> {
        open_arrow_access_storage::validate_file_names(files.iter().map(|file| file.name.as_str()))
            .map_err(|source| super::error::empty_or_dup_filename().with_source(source))?;
        Ok(Self {
            root: root.into(),
            files,
            metadata,
            query: Vec::new(),
        })
    }

    /// Query parameters apply to file downloads, not the metadata request.
    pub(crate) fn with_query(mut self, query: &[(&str, &str)]) -> Self {
        self.query = query
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect();
        self
    }
}

/// Connection and error context shared by public JSON-manifest providers.
#[derive(Clone, Debug)]
pub(crate) struct HttpFileService {
    endpoint: Url,
    client: HttpClient,
    /// Provider resolve operation (e.g. `resolve_zenodo`) for metadata HTTP/JSON errors.
    operation: &'static str,
    /// Maps empty/duplicate filenames to provider-specific decode errors,
    /// retaining the underlying namespace error as the source.
    map_file_namespace_error: fn(Error) -> Error,
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
            operation,
            map_file_namespace_error,
        })
    }

    pub(crate) fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Fetch one public JSON manifest, validate/map it, and bind the fixed file
    /// namespace over the same transport. Resolving and listing fetch no file bodies;
    /// namespace errors use the provider's existing context and source chain.
    pub(crate) async fn resolve_files<T: DeserializeOwned, M>(
        &self,
        metadata_url: &Url,
        resolve: impl FnOnce(T) -> Result<ResolvedFilesPlan<M>>,
    ) -> Result<ResolvedFileSet<OpenDalAccessor, M>> {
        let response = self
            .client
            .json(metadata_url.as_str(), /*token=*/ None, self.operation)
            .await?;
        let plan = resolve(response)?;
        let lengths = plan.files.iter().filter_map(|file| {
            file.content_length
                .map(|length| (file.object.locator().to_owned(), length))
        });
        let storage = self
            .client
            .accessor(&self.endpoint, &plan.root, plan.query)?
            .with_content_lengths(lengths);
        let files = NamedFileAccessor::new(
            storage,
            plan.files.into_iter().map(FileSpec::into_mapping).collect(),
        )
        .map_err(self.map_file_namespace_error)?;
        Ok(ResolvedFileSet::new(files, plan.metadata))
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
