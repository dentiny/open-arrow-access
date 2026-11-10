//! GovInfo bulk XML directories and API package discovery.
use crate::common::{ApiDocument, Connection, RemoteFiles, segment};
mod error;
mod resolver;
mod validation;
use open_arrow_access_core::Result;

#[derive(Clone, Debug)]
pub struct GovInfoService {
    bulk: Connection,
    api: Connection,
}
impl GovInfoService {
    /// No key is needed for bulk directories. A configured key enables API endpoints.
    pub fn new(api_key: Option<&str>) -> Result<Self> {
        let api = Connection::new("https://api.govinfo.gov")?;
        Ok(Self {
            bulk: Connection::new("https://www.govinfo.gov")?,
            api: if let Some(key) = api_key {
                api.with_query("api_key", key)
            } else {
                api
            },
        })
    }

    pub fn with_connections(bulk: Connection, api: Connection) -> Self {
        Self { bulk, api }
    }

    /// Resolve direct files in one bulk directory, retaining child folders in metadata.
    pub async fn bulk_directory(&self, prefix: &str) -> Result<RemoteFiles> {
        let prefix = resolver::resolve_prefix(prefix)?;
        self.bulk
            .resolve_json_files(&format!("bulkdata/json/{prefix}"), &[], |doc| {
                resolver::resolve_bulk_files(&self.bulk, &prefix, doc.value())
            })
            .await
    }

    pub async fn collections(&self) -> Result<ApiDocument> {
        self.api
            .json("collections", &[])
            .await?
            .validated(validation::validate_collections)
    }

    /// Fetch one last-modified interval page. `nextPage` is preserved verbatim.
    pub async fn packages(
        &self,
        collection: &str,
        start: &str,
        end: Option<&str>,
        offset_mark: &str,
        page_size: u16,
    ) -> Result<ApiDocument> {
        validation::validate_page_size(page_size)?;
        let mut path = format!("collections/{}/{}", segment(collection)?, segment(start)?);
        if let Some(end) = end {
            path.push('/');
            path.push_str(&segment(end)?);
        }
        let page_size = page_size.to_string();
        self.api
            .json(
                &path,
                &[("offsetMark", offset_mark), ("pageSize", &page_size)],
            )
            .await?
            .validated(validation::validate_packages)
    }

    /// Follow the API's returned nextPage link without allowing an origin change.
    pub async fn next_page(&self, next: &str) -> Result<ApiDocument> {
        self.api
            .json_url(crate::common::resolve_download_url(next)?)
            .await?
            .validated(validation::validate_packages)
    }

    /// Resolve all content and metadata representations advertised by a package.
    pub async fn package(&self, id: &str) -> Result<RemoteFiles> {
        self.api
            .resolve_json_files(&format!("packages/{}/summary", segment(id)?), &[], |doc| {
                resolver::resolve_package_files(doc.value(), id)
            })
            .await
    }
}
