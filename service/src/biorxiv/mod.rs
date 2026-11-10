//! bioRxiv and medRxiv metadata pages with lazy JATS access.
//!
//! Preserves observed API cursor order and validated JATS-link membership.
mod error;
mod parser;
mod resolver;
mod validation;
use crate::common::{ApiDocument, Connection, RemoteFiles};
pub use crate::meca::{MecaArchive, MecaFile};
use open_arrow_access_core::Result;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Server {
    Biorxiv,
    Medrxiv,
}
impl Server {
    pub fn name(self) -> &'static str {
        match self {
            Self::Biorxiv => "biorxiv",
            Self::Medrxiv => "medrxiv",
        }
    }

    /// Public bulk bucket; request-payer configuration remains with the storage caller.
    pub fn bulk_bucket(self) -> &'static str {
        match self {
            Self::Biorxiv => "biorxiv-src-monthly",
            Self::Medrxiv => "medrxiv-src-monthly",
        }
    }
}
#[derive(Clone, Debug)]
pub struct BiorxivPage {
    pub response: ApiDocument,
    pub next_cursor: Option<u64>,
}
#[derive(Clone, Debug)]
pub struct BiorxivService {
    connection: Connection,
}
impl BiorxivService {
    pub fn new() -> Result<Self> {
        Ok(Self::with_connection(Connection::new(
            "https://api.biorxiv.org",
        )?))
    }

    pub fn with_connection(connection: Connection) -> Self {
        Self { connection }
    }

    /// One page, including all returned versions and licenses without filtering.
    pub async fn details(
        &self,
        server: Server,
        from: &str,
        until: &str,
        cursor: u64,
    ) -> Result<BiorxivPage> {
        let from_date = parser::parse_date(from)?;
        let until_date = parser::parse_date(until)?;
        validation::validate_date_range(from_date, until_date)?;
        let response = self
            .connection
            .json(
                &format!("details/{}/{from}/{until}/{cursor}/json", server.name()),
                &[],
            )
            .await?;
        let next_cursor = parser::parse_next_cursor(response.value(), cursor)?;
        Ok(BiorxivPage {
            response,
            next_cursor,
        })
    }

    /// Bind the original page to the advertised JATS XML links. Missing links retain metadata.
    pub fn jats(&self, page: BiorxivPage) -> Result<RemoteFiles<BiorxivPage>> {
        let files = resolver::resolve_jats_files(page.response.value())?;
        self.connection.files(files, page)
    }
}
