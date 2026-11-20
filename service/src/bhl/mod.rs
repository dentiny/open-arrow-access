//! BHL API v3 and its versioned Smithsonian Figshare OCR export.
mod error;
mod open_data;
mod pages;
pub use pages::{BhlPageAccessor, BhlPageIdentity, page_schema};
mod resolver;
mod validation;
use crate::{
    common::{ApiDocument, ApiKey, Connection, RemoteFiles},
    figshare::FigshareService,
};
use open_arrow_access_core::Result;
pub use open_data::{BhlOpenData, OPEN_DATA_BUCKET_URL, open_data_bucket_operator};
use std::num::{NonZeroU32, NonZeroU64};
#[derive(Clone, Copy, Debug)]
pub enum BhlOperation {
    TitleMetadata,
    ItemMetadata,
    PageMetadata,
    PartMetadata,
    Collections,
    Stats,
}
impl BhlOperation {
    fn name(self) -> &'static str {
        match self {
            Self::TitleMetadata => "GetTitleMetadata",
            Self::ItemMetadata => "GetItemMetadata",
            Self::PageMetadata => "GetPageMetadata",
            Self::PartMetadata => "GetPartMetadata",
            Self::Collections => "GetCollections",
            Self::Stats => "GetStats",
        }
    }
}
#[derive(Clone, Debug)]
pub struct BhlService {
    connection: Connection,
    figshare: FigshareService,
}
impl BhlService {
    /// The bulk export needs no key. API v3 calls require a configured BHL key.
    pub fn new(key: Option<&str>) -> Result<Self> {
        let connection = Connection::new("https://www.biodiversitylibrary.org")?;
        let service = Self::with_services(connection, FigshareService::new()?);
        if let Some(key) = key {
            service.with_key(key)
        } else {
            Ok(service)
        }
    }

    /// Set or replace the API v3 key without changing the configured API or
    /// Figshare connections. Query credentials stay scoped to the API origin.
    pub fn with_key(mut self, key: &str) -> Result<Self> {
        self.connection = self.connection.with_api_key_message(
            ApiKey::Query("apikey"),
            key,
            "empty BHL API key",
        )?;
        Ok(self)
    }

    pub fn with_services(connection: Connection, figshare: FigshareService) -> Self {
        Self {
            connection,
            figshare,
        }
    }

    pub async fn bulk_export(&self, version: Option<NonZeroU32>) -> Result<RemoteFiles> {
        self.figshare
            .article(NonZeroU64::new(21422193).unwrap(), version)
            .await
    }

    /// Fetch one read-only API response, preserving every upstream field.
    pub async fn get(
        &self,
        operation: BhlOperation,
        parameters: &[(&str, &str)],
    ) -> Result<ApiDocument> {
        validation::validate_parameters(parameters)?;
        let mut query = vec![("op", operation.name()), ("format", "json")];
        query.extend_from_slice(parameters);
        self.connection
            .json("api3", &query)
            .await?
            .validated(validation::validate_response)
    }

    pub async fn item(&self, id: NonZeroU64) -> Result<RemoteFiles> {
        let id_text = id.to_string();
        let doc = self
            .get(
                BhlOperation::ItemMetadata,
                &[
                    ("id", &id_text),
                    ("idtype", "bhl"),
                    ("pages", "f"),
                    ("ocr", "f"),
                    ("parts", "f"),
                ],
            )
            .await?;
        let files = resolver::resolve_item_files(doc.value(), id.get())?;
        self.connection.files(files, doc)
    }
}
