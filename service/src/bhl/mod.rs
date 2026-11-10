//! BHL API v3 and its versioned Smithsonian Figshare OCR export.
//!
//! Resolved files expose `records(format)` for explicit Arrow decoding and scan
//! planning. The adapter retains validated BHL item identity and versioned OCR-export membership;
//! metadata links are not followed automatically. API pages can bind an explicit
//! JSON record selection with `ApiDocument::records_at`.
mod error;
mod resolver;
mod validation;
use crate::{
    common::{ApiDocument, Connection, RemoteFiles},
    figshare::FigshareService,
};
use open_arrow_access_core::Result;
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
        let connection = if let Some(key) = key {
            validation::validate_key(key)?;
            connection.with_query("apikey", key)
        } else {
            connection
        };
        Ok(Self::with_services(connection, FigshareService::new()?))
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
