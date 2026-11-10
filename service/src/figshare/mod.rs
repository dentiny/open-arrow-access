//! Figshare public versioned articles, including BHL's Smithsonian OCR export.
//!
//! Resolved files expose `records(format)` for explicit Arrow decoding and scan
//! planning. The adapter retains article identity, published version and embargo validation;
//! metadata links are not followed automatically. API pages can bind an explicit
//! JSON record selection with `ApiDocument::records_at`.
mod error;
mod parser;
mod validation;
use crate::common::{ApiDocument, Connection, RemoteFiles};
use open_arrow_access_core::Result;
use std::num::{NonZeroU32, NonZeroU64};
#[derive(Clone, Debug)]
pub struct FigshareService {
    connection: Connection,
}
impl FigshareService {
    pub fn new() -> Result<Self> {
        Ok(Self::with_connection(Connection::new(
            "https://api.figshare.com",
        )?))
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn article(
        &self,
        id: NonZeroU64,
        version: Option<NonZeroU32>,
    ) -> Result<RemoteFiles> {
        let mut path = format!("v2/articles/{id}");
        if let Some(version) = version {
            path.push_str(&format!("/versions/{version}"));
        }
        self.connection
            .resolve_json_files(&path, &[], |doc| {
                parser::parse_article_files(doc.value(), id.get(), version.map(NonZeroU32::get))
            })
            .await
    }

    pub async fn collection_articles(
        &self,
        id: NonZeroU64,
        page: u32,
        page_size: u16,
    ) -> Result<ApiDocument> {
        validation::validate_pagination(page, page_size)?;
        let page = page.to_string();
        let page_size = page_size.to_string();
        self.connection
            .json(
                &format!("v2/collections/{id}/articles"),
                &[("page", &page), ("page_size", &page_size)],
            )
            .await?
            .validated(validation::validate_collection)
    }
}
