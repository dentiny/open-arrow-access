use super::{OaiPage, parser, validation};
use crate::common::Connection;
use open_arrow_access_core::Result;
use url::Url;

/// Protocol shared by arXiv, DOAB and OAPEN. One method call fetches one page.
#[derive(Clone, Debug)]
pub struct OaiService {
    connection: Connection,
    endpoint: Url,
    metadata_prefix: String,
}
impl OaiService {
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub fn new(connection: Connection, path: &str, metadata_prefix: &str) -> Result<Self> {
        let endpoint = connection.url(path, &[])?;
        validation::validate_nonempty_text(metadata_prefix, "metadata prefix")?;
        Ok(Self {
            connection,
            endpoint,
            metadata_prefix: metadata_prefix.into(),
        })
    }

    pub async fn records(
        &self,
        from: Option<&str>,
        until: Option<&str>,
        set: Option<&str>,
    ) -> Result<OaiPage> {
        let mut query = vec![
            ("verb", "ListRecords"),
            ("metadataPrefix", &self.metadata_prefix),
        ];
        for (key, value) in [("from", from), ("until", until), ("set", set)] {
            if let Some(value) = value {
                validation::validate_nonempty_text(value, "OAI selector")?;
                query.push((key, value));
            }
        }
        self.page(&query).await
    }

    /// Resumption requests contain only verb and token, as required by OAI-PMH.
    pub async fn resume(&self, token: &str) -> Result<OaiPage> {
        validation::validate_nonempty_text(token, "resumption token")?;
        self.page(&[("verb", "ListRecords"), ("resumptionToken", token)])
            .await
    }

    async fn page(&self, query: &[(&str, &str)]) -> Result<OaiPage> {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        let bytes = self.connection.bytes(url, "application/xml").await?;
        parser::parse_page(bytes)
    }
}
