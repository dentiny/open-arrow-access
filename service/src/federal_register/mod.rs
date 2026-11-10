//! Federal Register public API pages and original document representations.
//!
//! Preserves validated document numbers and available representation links.
mod error;
mod resolver;
mod validation;
use crate::common::{ApiDocument, Connection, RemoteFiles, resolve_download_url, segment};
use open_arrow_access_core::Result;
#[derive(Clone, Debug)]
pub struct FederalRegisterService {
    connection: Connection,
}
impl FederalRegisterService {
    pub fn new() -> Result<Self> {
        Ok(Self::with_connection(Connection::new(
            "https://www.federalregister.gov",
        )?))
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn search(&self, parameters: &[(&str, &str)]) -> Result<ApiDocument> {
        self.connection
            .json("api/v1/documents.json", parameters)
            .await?
            .validated(validation::validate_search)
    }

    /// Follow the supplied next_page_url intact, including cursor query parameters.
    pub async fn next_page(&self, url: &str) -> Result<ApiDocument> {
        self.connection
            .json_url(resolve_download_url(url)?)
            .await?
            .validated(validation::validate_search)
    }

    pub async fn document(&self, number: &str) -> Result<RemoteFiles> {
        self.connection
            .resolve_json_files(
                &format!("api/v1/documents/{}.json", segment(number)?),
                &[],
                |doc| resolver::resolve_document_files(doc.value(), number),
            )
            .await
    }
}
