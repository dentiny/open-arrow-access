//! OpenStax book repositories pinned to Git commit snapshots.
//!
//! Resolved files expose `records(format)` for explicit Arrow decoding and scan
//! planning. The adapter retains resolved Git commit identity and commit-bound archive URLs;
//! metadata links are not followed automatically. API pages can bind an explicit
//! JSON record selection with `ApiDocument::records_at`.
mod error;
mod parser;
mod resolver;
mod validation;
use crate::common::{ApiDocument, Connection, MetadataLink, RemoteFiles, segment};
use open_arrow_access_core::Result;
#[derive(Clone, Debug)]
pub struct OpenStaxService {
    api: Connection,
    archives: Connection,
}
#[derive(Clone, Debug)]
pub struct BookMetadata {
    pub repository: String,
    pub commit_sha: String,
    pub commit: ApiDocument,
}
impl OpenStaxService {
    pub fn new() -> Result<Self> {
        Ok(Self::with_connections(
            Connection::new("https://api.github.com")?,
            Connection::new("https://codeload.github.com")?,
        ))
    }

    pub fn with_connections(api: Connection, archives: Connection) -> Self {
        Self { api, archives }
    }

    pub async fn repositories(&self, page: u32, per_page: u16) -> Result<ApiDocument> {
        validation::validate_pagination(page, per_page)?;
        let page = page.to_string();
        let per_page = per_page.to_string();
        self.api
            .json(
                "orgs/openstax/repos",
                &[("type", "public"), ("page", &page), ("per_page", &per_page)],
            )
            .await?
            .validated(validation::validate_repositories)
    }

    /// Resolve a branch/tag once; downloads always use the resolved commit SHA.
    pub async fn book(
        &self,
        repository: &str,
        revision: &str,
    ) -> Result<RemoteFiles<BookMetadata>> {
        let repo = resolver::resolve_repository(repository)?;
        let commit = self
            .api
            .json(
                &format!("repos/openstax/{repo}/commits/{}", segment(revision)?),
                &[],
            )
            .await?;
        let sha = parser::parse_commit_sha(commit.value())?.to_owned();
        let url = self
            .archives
            .url(&format!("openstax/{repo}/zip/{sha}"), &[])?;
        self.archives.files(
            vec![MetadataLink {
                file_name: format!("{repository}.zip"),
                url: url.into(),
            }],
            BookMetadata {
                repository: repository.into(),
                commit_sha: sha,
                commit,
            },
        )
    }
}
