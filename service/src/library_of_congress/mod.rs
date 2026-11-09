//! Library of Congress public metadata, resource files and original OCR responses.
mod error;
mod resolver;
mod validation;
use crate::common::{ApiDocument, Connection, RemoteFiles};
use open_arrow_access_core::Result;
#[derive(Clone, Debug)]
pub struct LibraryOfCongressService {
    api: Connection,
    text: Connection,
}
impl LibraryOfCongressService {
    pub fn new() -> Result<Self> {
        Ok(Self::with_connections(
            Connection::new("https://www.loc.gov")?,
            Connection::new("https://tile.loc.gov")?,
        ))
    }

    pub fn with_connections(api: Connection, text: Connection) -> Self {
        Self { api, text }
    }

    pub async fn search(
        &self,
        parameters: &[(&str, &str)],
        page: u32,
        count: u16,
    ) -> Result<ApiDocument> {
        validation::validate_search_request(parameters, page, count)?;
        let page = page.to_string();
        let count = count.to_string();
        let mut query = vec![("fo", "json"), ("sp", &page), ("c", &count)];
        query.extend_from_slice(parameters);
        self.api
            .json("search/", &query)
            .await?
            .validated(validation::validate_search_response)
    }

    pub async fn next_page(&self, url: &str) -> Result<ApiDocument> {
        let url = resolver::resolve_continuation_url(&self.api, url)?;
        self.api
            .json_url(url)
            .await?
            .validated(validation::validate_search_response)
    }

    /// Accept a local item ID or the upstream item.id URL, including historical HTTP IDs.
    pub async fn item(&self, id: &str) -> Result<ApiDocument> {
        self.api
            .json_url(resolver::resolve_metadata_url(&self.api, "item", id)?)
            .await?
            .validated(validation::validate_item_response)
    }

    pub async fn resource(&self, id: &str) -> Result<RemoteFiles> {
        let doc = self
            .api
            .json_url(resolver::resolve_metadata_url(&self.api, "resource", id)?)
            .await?;
        let files = resolver::resolve_resource_files(doc.value())?;
        self.api.files(files, doc)
    }

    /// Request full_text=1 and retain the entire text-services JSON response unchanged.
    pub async fn ocr(&self, segment: &str, format: &str) -> Result<ApiDocument> {
        validation::validate_ocr_request(segment, format)?;
        self.text
            .json(
                "text-services/word-coordinates-service",
                &[("segment", segment), ("format", format), ("full_text", "1")],
            )
            .await?
            .validated(|value| validation::validate_ocr_response(value, segment))
    }
}
