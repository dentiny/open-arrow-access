//! Read-only Regulations.gov API v4 discovery, metadata and attachments.
mod parser;
mod validation;
use crate::common::{ApiDocument, Connection, RemoteFiles, invalid, segment};
use open_arrow_access_core::Result;
#[derive(Clone, Copy, Debug)]
pub enum RegulationsResource {
    Documents,
    Comments,
    Dockets,
}
impl RegulationsResource {
    pub fn name(self) -> &'static str {
        match self {
            Self::Documents => "documents",
            Self::Comments => "comments",
            Self::Dockets => "dockets",
        }
    }
}
#[derive(Clone, Debug)]
pub struct RegulationsService {
    connection: Connection,
}
impl RegulationsService {
    pub fn new(key: &str) -> Result<Self> {
        if key.is_empty() {
            return Err(invalid("Regulations.gov requires an API key"));
        }
        Ok(Self::with_connection(
            Connection::new("https://api.regulations.gov")?.with_header("X-Api-Key", key)?,
        ))
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    /// Read one bounded API page. Filters and sort selectors use upstream names.
    pub async fn search(
        &self,
        resource: RegulationsResource,
        parameters: &[(&str, &str)],
        page: u8,
        page_size: u16,
    ) -> Result<ApiDocument> {
        validation::validate_pagination(parameters, page, page_size)?;
        let page = page.to_string();
        let size = page_size.to_string();
        let mut query = parameters.to_vec();
        query.extend_from_slice(&[("page[number]", &page), ("page[size]", &size)]);
        let doc = self
            .connection
            .json(&format!("v4/{}", resource.name()), &query)
            .await?;
        validation::validate_search_response(doc.value())?;
        Ok(doc)
    }

    pub async fn record(&self, resource: RegulationsResource, id: &str) -> Result<RemoteFiles> {
        let query: &[(&str, &str)] = match resource {
            RegulationsResource::Dockets => &[],
            _ => &[("include", "attachments")],
        };
        self.connection
            .resolve_json_files(
                &format!("v4/{}/{}", resource.name(), segment(id)?),
                query,
                |doc| parser::parse_record_files(doc.value(), resource, id),
            )
            .await
    }
}
